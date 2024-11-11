use anyhow::{anyhow, bail, Context, Result};
use async_nats::jetstream::consumer::DeliverPolicy;
use async_nats::Message as NatsMessage;
use futures_util::stream::SelectAll;
use p2panda_net::network::FromNetwork;
use p2panda_net::TopicId;
use rhio_core::message::NetworkPayload;
use rhio_core::{NetworkMessage, ScopedSubject};
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;
use tracing::error;

use crate::blobs::Blobs;
use crate::nats::{JetStreamEvent, Nats};
use crate::network::Panda;
use crate::node::Publication;
use crate::topic::{Query, Subscription};

pub enum ToNodeActor {
    Publish {
        publication: Publication,
        reply: oneshot::Sender<Result<()>>,
    },
    Subscribe {
        subscription: Subscription,
        reply: oneshot::Sender<Result<()>>,
    },
    Shutdown {
        reply: oneshot::Sender<()>,
    },
}

pub struct NodeActor {
    inbox: mpsc::Receiver<ToNodeActor>,
    subscriptions: Vec<Subscription>,
    nats_consumer_rx: SelectAll<BroadcastStream<JetStreamEvent>>,
    p2panda_topic_rx: SelectAll<BroadcastStream<FromNetwork>>,
    nats: Nats,
    panda: Panda,
    blobs: Blobs,
}

impl NodeActor {
    pub fn new(nats: Nats, panda: Panda, blobs: Blobs, inbox: mpsc::Receiver<ToNodeActor>) -> Self {
        Self {
            nats,
            subscriptions: Vec::new(),
            nats_consumer_rx: SelectAll::new(),
            p2panda_topic_rx: SelectAll::new(),
            panda,
            blobs,
            inbox,
        }
    }

    pub async fn run(mut self) -> Result<()> {
        // Take oneshot sender from external API awaited by `shutdown` call and fire it as soon as
        // shutdown completed to signal
        let shutdown_completed_signal = self.run_inner().await;
        if let Err(err) = self.shutdown().await {
            error!(?err, "error during shutdown");
        }

        drop(self);

        match shutdown_completed_signal {
            Ok(reply_tx) => {
                reply_tx.send(()).ok();
                Ok(())
            }
            Err(err) => Err(err),
        }
    }

    async fn run_inner(&mut self) -> Result<oneshot::Sender<()>> {
        loop {
            tokio::select! {
                biased;
                Some(msg) = self.inbox.recv() => {
                    match msg {
                        ToNodeActor::Shutdown { reply } => {
                            break Ok(reply);
                        }
                        msg => {
                            if let Err(err) = self.on_actor_message(msg).await {
                                break Err(err);
                            }
                        }
                    }
                },
                Some(Ok(event)) = self.nats_consumer_rx.next() => {
                    if let Err(err) = self.on_nats_event(event).await {
                        break Err(err);
                    }
                },
                Some(Ok(event)) = self.p2panda_topic_rx.next() => {
                    if let Err(err) = self.on_network_event(event).await {
                        break Err(err);
                    }
                },
                else => {
                    // Error occurred outside of actor and our select! loop got disabled. We exit
                    // here with an error which will probably be overriden by the external error
                    // which caused the problem in first hand.
                    break Err(anyhow!("all select! branches are disabled"));
                }
            }
        }
    }

    async fn on_actor_message(&mut self, msg: ToNodeActor) -> Result<()> {
        match msg {
            ToNodeActor::Publish { publication, reply } => {
                let result = self.on_publish(publication).await;
                reply.send(result).ok();
            }
            ToNodeActor::Subscribe {
                subscription,
                reply,
            } => {
                let result = self.on_subscribe(subscription).await;
                reply.send(result).ok();
            }
            ToNodeActor::Shutdown { .. } => {
                unreachable!("handled in run_inner");
            }
        }

        Ok(())
    }

    async fn on_publish(&mut self, publication: Publication) -> Result<()> {
        let topic_query: Query = publication.clone().into();
        let topic_id = topic_query.id();
        let network_rx = self.panda.subscribe(topic_query).await?;

        let nats_rx = match publication {
            Publication::Bucket { bucket_name: _ } => todo!(),
            Publication::Subject {
                ref stream_name,
                ref subject,
            } => {
                self.nats
                    .subscribe(
                        stream_name.clone(),
                        subject.clone(),
                        DeliverPolicy::New,
                        topic_id,
                    )
                    .await?
            }
        };

        // Wrap broadcast receiver stream into tokio helper, to make it implement the `Stream`
        // trait which is required by `SelectAll`.
        self.nats_consumer_rx.push(BroadcastStream::new(nats_rx));
        let p2panda_topic_rx = BroadcastStream::new(network_rx);

        // @TODO: Messages coming from the p2p gossip overlay do not necessarily fit the NATS
        // subject filtering logic and need to be excluded here.
        self.p2panda_topic_rx.push(p2panda_topic_rx);

        Ok(())
    }

    /// Callback when the application decided to subscribe to a new NATS message stream or S3
    /// bucket.
    async fn on_subscribe(&mut self, subscription: Subscription) -> Result<()> {
        self.subscriptions.push(subscription.clone());
        let network_rx = self.panda.subscribe(subscription.into()).await?;
        self.p2panda_topic_rx.push(BroadcastStream::new(network_rx));
        Ok(())
    }

    /// Handler for incoming events from the NATS stream consumer.
    async fn on_nats_event(&mut self, event: JetStreamEvent) -> Result<()> {
        match event {
            JetStreamEvent::Message {
                topic_id, message, ..
            } => {
                self.on_nats_message(topic_id, message).await?;
            }
            JetStreamEvent::StreamFailed {
                stream_name,
                reason,
                ..
            } => {
                bail!("stream '{}' failed: {}", stream_name, reason);
            }
            JetStreamEvent::InitCompleted { .. } => {
                // We do not handle sync sessions here but this event get's anyhow called, even
                // when we're setting `DeliverPolicy` to `New`. This is why we're simply just
                // ignoring it ..
            }
            JetStreamEvent::InitFailed { .. } => {
                // .. though an error during "init" we should definitely never receive.
                unreachable!("we do not handle sync sessions here");
            }
        }

        Ok(())
    }

    /// Handler for incoming messages from the NATS JetStream consumer.
    ///
    /// From here we're broadcasting the NATS messages in the related gossip overlay network.
    async fn on_nats_message(&mut self, topic_id: [u8; 32], message: NatsMessage) -> Result<()> {
        let network_message = NetworkMessage::new_nats(message);
        self.panda
            .broadcast(network_message.to_bytes(), topic_id)
            .await
            .context("broadcast incoming operation from nats")?;
        Ok(())
    }

    /// Handler for incoming events from the p2p network.
    async fn on_network_event(&mut self, event: FromNetwork) -> Result<()> {
        let bytes = match event {
            FromNetwork::GossipMessage { bytes, .. } => bytes,
            FromNetwork::SyncMessage { header, .. } => header,
        };

        let network_message = NetworkMessage::from_bytes(&bytes)?;
        match network_message.payload {
            NetworkPayload::BlobAnnouncement(_scoped_bucket) => todo!(),
            NetworkPayload::NatsMessage(message) => {
                // Filter out all incoming messages we're not subscribed to. This can happen
                // especially when receiving messages over the gossip overlay as they are not
                // necessarily for us.
                let subject = message.subject.clone().try_into()?;
                if !is_matching(&self.subscriptions, &subject) {
                    return Ok(());
                }

                self.nats
                    .publish(
                        true,
                        message.subject.to_string(),
                        message.headers,
                        message.payload.to_vec(),
                    )
                    .await?;
            }
        }

        Ok(())
    }

    async fn shutdown(&self) -> Result<()> {
        self.nats.shutdown().await?;
        self.panda.shutdown().await?;
        self.blobs.shutdown().await?;
        Ok(())
    }
}

fn is_matching(subscriptions: &Vec<Subscription>, incoming: &ScopedSubject) -> bool {
    for subscription in subscriptions {
        match subscription {
            Subscription::Bucket { .. } => continue,
            Subscription::Subject { subject, .. } => {
                if subject.is_matching(incoming) {
                    return true;
                } else {
                    continue;
                }
            }
        }
    }
    false
}
