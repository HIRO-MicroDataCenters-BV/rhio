use std::{collections::HashMap, marker::PhantomData, sync::Arc};

use anyhow::{Context, Result};
use async_nats::jetstream::consumer::DeliverPolicy;
use async_nats::HeaderMap;
use rand::random;
use rhio_core::{subjects_to_str, Subject};
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, error, trace};

use crate::nats::{
    client::types::NatsClient,
    consumer::{Consumer, ConsumerId, JetStreamEvent, StreamName},
};

use super::client::types::NatsMessageStream;

pub enum ToNatsActor {
    Publish {
        /// Wait for acknowledgment of NATS JetStream.
        ///
        /// Important: If we're sending a regular NATS Core message (for example during a
        /// request-response flow), messages will _never_ be acknowledged. In this case this flag
        /// should be set to false.
        wait_for_ack: bool,

        /// NATS subject to which this message is published to.
        subject: String,

        /// Payload of message.
        payload: Vec<u8>,

        /// NATS headers of message.
        headers: Option<HeaderMap>,

        /// Channel to receive result. Can fail if server did not acknowledge message in time.
        reply: oneshot::Sender<Result<()>>,
    },
    Subscribe {
        /// NATS stream name.
        ///
        /// Streams need to already be created on the server, if not, this method will fail here.
        /// Note that no checks are applied here for validating if the NATS stream configuration is
        /// compatible with rhio's design.
        stream_name: StreamName,

        /// NATS subject filter configuration for the stream consumer.
        ///
        /// Streams can hold different subjects. By using a "subject filter" we're able to
        /// "consume" only the ones we're interested in. This forms "filtered views" on top of
        /// streams.
        ///
        /// Multiple subjects can be applied per JetStream.
        subjects: Vec<Subject>,

        /// Consumer delivery policy.
        ///
        /// For rhio two different delivery policies are configured:
        ///
        /// 1. Live-Mode: We're only interested in _upcoming_ messages as this consumer will only
        ///    be used to forward NATS messages into the gossip overlay. This happens when a rhio
        ///    node decided to "publish" a NATS subject, the created consumer lives as long as the
        ///    process.
        /// 2. Sync-Session: Here we want to load and exchange _past_ messages, usually loading all
        ///    messages from after a given timestamp. This happens when a remote rhio node requests
        ///    data from a NATS subject from us, the created consumer lives as long as the sync
        ///    session with this remote peer.
        deliver_policy: DeliverPolicy,

        /// p2panda topic id used to exchange the "filtered" NATS stream with external nodes over a
        /// gossip overlay.
        ///
        /// While this is not strictly required for NATS JetStream we keep it here to inform other
        /// parts of rhio about which topic id is used for this stream.
        topic_id: [u8; 32],

        /// Channel to receive all messages (old and new) from this subscription, including
        /// errors and "readiness" state.
        ///
        /// An initial downloading of all persisted data from the NATS server is required when
        /// starting to subscribe to a subject. The channel will eventually send an event to the
        /// user to signal when the initialization has finished.
        reply: oneshot::Sender<Result<(ConsumerId, loole::Receiver<JetStreamEvent>)>>,
    },
    Unsubscribe {
        consumer_id: ConsumerId,
    },
    Shutdown {
        reply: oneshot::Sender<()>,
    },
}

pub struct NatsActor<M, N>
where
    M: NatsMessageStream + Send + Sync + Unpin + 'static,
    N: NatsClient<M>,
{
    inbox: mpsc::Receiver<ToNatsActor>,
    nats_client: Arc<N>,
    consumers: HashMap<ConsumerId, Consumer>,
    phantom: PhantomData<M>,
}

impl<M, N> NatsActor<M, N>
where
    M: NatsMessageStream + Send + Sync + Unpin + 'static,
    N: NatsClient<M> + 'static + Send + Sync,
{
    pub fn new(nats_client: N, inbox: mpsc::Receiver<ToNatsActor>) -> Self {
        Self {
            inbox,
            nats_client: Arc::new(nats_client),
            consumers: HashMap::new(),
            phantom: PhantomData,
        }
    }

    pub async fn run(mut self) -> Result<()> {
        // Take oneshot sender from outside API awaited by `shutdown` call and fire it as soon as
        // shutdown completed
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
                msg = self.inbox.recv() => {
                    let msg = msg.context("inbox closed")?;
                    match msg {
                        ToNatsActor::Shutdown { reply } => {
                            break Ok(reply);
                        }
                        msg => {
                            if let Err(err) = self.on_actor_message(msg).await {
                                break Err(err);
                            }
                        }
                    }
                },
            }
        }
    }

    async fn on_actor_message(&mut self, msg: ToNatsActor) -> Result<()> {
        match msg {
            ToNatsActor::Publish {
                wait_for_ack,
                subject,
                headers,
                payload,
                reply,
            } => {
                let result = self
                    .on_publish(wait_for_ack, subject, headers, payload)
                    .await;
                reply.send(result).ok();
            }
            ToNatsActor::Subscribe {
                stream_name,
                subjects,
                deliver_policy,
                topic_id,
                reply,
            } => {
                let result = self
                    .on_subscribe(stream_name, subjects, deliver_policy, topic_id)
                    .await;
                reply.send(result).ok();
            }
            ToNatsActor::Unsubscribe { consumer_id } => {
                self.on_unsubscribe(consumer_id).await?;
            }
            ToNatsActor::Shutdown { .. } => {
                unreachable!("handled in run_inner");
            }
        }

        Ok(())
    }

    /// Publish a message to the NATS server.
    async fn on_publish(
        &self,
        wait_for_ack: bool,
        subject: String,
        headers: Option<HeaderMap>,
        payload: Vec<u8>,
    ) -> Result<()> {
        debug!(%subject, ?payload, bytes = payload.len(), "publish NATS message");

        self.nats_client
            .publish(wait_for_ack, subject, payload.into(), headers)
            .await?;

        Ok(())
    }

    async fn on_subscribe(
        &mut self,
        stream_name: StreamName,
        filter_subjects: Vec<Subject>,
        deliver_policy: DeliverPolicy,
        topic_id: [u8; 32],
    ) -> Result<(ConsumerId, loole::Receiver<JetStreamEvent>)> {
        let filter_subjects_str = subjects_to_str(filter_subjects.clone());

        match deliver_policy {
            DeliverPolicy::All => {
                // Consumers who are used to download _all_ messages are only used once per sync
                // session. We shouldn't re-use them later, as every sync session wants to download
                // all messages again. This is why we a) give them a random identifier to avoid
                // re-use b) do not allow to re-subscribe to it.
                let random_id: u32 = random();
                let consumer_id = ConsumerId::new(
                    stream_name.clone(),
                    format!("{filter_subjects_str}-{random_id}"),
                );

                let mut consumer = Consumer::new(
                    self.nats_client.clone(),
                    consumer_id.clone(),
                    deliver_policy,
                    filter_subjects,
                    stream_name,
                    topic_id,
                )
                .await?;

                trace!(
                    "Subscribe new consumer with DeliveryPolicy=All id={}",
                    consumer_id.to_string()
                );
                let rx = consumer.subscribe();
                self.consumers.insert(consumer_id.clone(), consumer);
                Ok((consumer_id, rx))
            }
            DeliverPolicy::New => {
                // Make sure we're only creating one consumer per stream name and subjects pair
                // when using NATS consumer for _new_ messages.
                let consumer_id = ConsumerId::new(stream_name.clone(), filter_subjects_str);
                let rx = match self.consumers.get_mut(&consumer_id) {
                    Some(consumer) => {
                        trace!(
                            "Subscribe to existing consumer with DeliveryPolicy=New id={}",
                            consumer_id.to_string()
                        );
                        consumer.subscribe()
                    }
                    None => {
                        let mut consumer = Consumer::new(
                            self.nats_client.clone(),
                            consumer_id.clone(),
                            deliver_policy,
                            filter_subjects,
                            stream_name,
                            topic_id,
                        )
                        .await?;
                        trace!(
                            "Subscribe new consumer with DeliveryPolicy=New id={}",
                            consumer_id.to_string()
                        );
                        let rx = consumer.subscribe();
                        self.consumers.insert(consumer_id.clone(), consumer);
                        rx
                    }
                };
                Ok((consumer_id, rx))
            }
            _ => unimplemented!("other delivery policies are not used in rhio"),
        }
    }

    async fn on_unsubscribe(&mut self, consumer_id: ConsumerId) -> Result<()> {
        trace!("Unsubscribe consumer {}", consumer_id.to_string());
        self.consumers.remove(&consumer_id);
        Ok(())
    }

    async fn shutdown(&mut self) -> Result<()> {
        Ok(())
    }
}
