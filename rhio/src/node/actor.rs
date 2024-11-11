use anyhow::{anyhow, bail, Context, Result};
use async_nats::jetstream::consumer::DeliverPolicy;
use futures_util::stream::SelectAll;
use p2panda_net::network::FromNetwork;
use p2panda_net::TopicId;
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
            Publication::Bucket { bucket_name } => todo!(),
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
        self.p2panda_topic_rx.push(BroadcastStream::new(network_rx));

        Ok(())
    }

    /// Callback when the application decided to subscribe to a new NATS message stream or S3
    /// bucket.
    async fn on_subscribe(&mut self, subscription: Subscription) -> Result<()> {
        // self.panda.subscribe(topic)

        // @TODO: Remove this
        // let nats_rx = self
        //     .nats
        //     .subscribe(stream_name, filter_subject, topic)
        //     .await?;
        // // Wrap broadcast receiver stream into tokio helper, to make it implement the `Stream`
        // // trait which is required by `SelectAll`
        // self.nats_consumer_rx.push(BroadcastStream::new(nats_rx));

        Ok(())
    }

    /// Handler for incoming events from the NATS stream consumer.
    async fn on_nats_event(&mut self, event: JetStreamEvent) -> Result<()> {
        match event {
            JetStreamEvent::Message {
                topic_id, payload, ..
            } => {
                self.on_nats_message(topic_id, payload).await?;
            }
            JetStreamEvent::StreamFailed {
                stream_name,
                reason,
                ..
            } => {
                bail!("stream '{}' failed: {}", stream_name, reason);
            }
            JetStreamEvent::InitCompleted { .. } => {
                unreachable!("we do not handle sync sessions here");
            }
            JetStreamEvent::InitFailed { .. } => {
                unreachable!("we do not handle sync sessions here");
            }
        }

        Ok(())
    }

    /// Handler for incoming messages from the NATS JetStream consumer.
    ///
    /// From here we're broadcasting the NATS messages in the related gossip overlay network.
    async fn on_nats_message(&mut self, topic_id: [u8; 32], payload: Vec<u8>) -> Result<()> {
        self.panda
            .broadcast(payload, topic_id)
            .await
            .context("broadcast incoming operation from nats")?;
        Ok(())
    }

    /// Handler for incoming events from the p2p network.
    async fn on_network_event(&mut self, event: FromNetwork) -> Result<()> {
        // self.nats.publish(true, subject, payload).await?;
        Ok(())
    }

    async fn shutdown(&self) -> Result<()> {
        self.nats.shutdown().await?;
        self.panda.shutdown().await?;
        self.blobs.shutdown().await?;
        Ok(())
    }
}
