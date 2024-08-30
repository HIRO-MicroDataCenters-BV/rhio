use std::collections::HashMap;

use anyhow::{bail, Context, Result};
use async_nats::jetstream::Context as JetstreamContext;
use async_nats::Client as NatsClient;
use rhio_core::{Subject, TopicId};
use tokio::sync::{broadcast, mpsc, oneshot};
use tracing::error;

use crate::nats::consumer::{Consumer, ConsumerId, JetStreamEvent};

pub enum ToNatsActor {
    Publish {
        /// Wait for acknowledgment of NATS JetStream.
        ///
        /// Important: If we're sending a regular NATS Core message (for example during a
        /// request-response flow), messages will _never_ be acknowledged. In this case this flag
        /// should be set to false.
        wait_for_ack: bool,

        /// NATS subject to which this message is published to.
        subject: Subject,

        /// Payload of message.
        payload: Vec<u8>,

        /// Channel to receive result. Can fail if server did not acknowledge message in time.
        reply: oneshot::Sender<Result<()>>,
    },
    Subscribe {
        /// NATS stream name.
        stream_name: String,

        /// NATS subject filter.
        ///
        /// Streams can hold different subjects. By using a "subject filter" we're able to select only
        /// the ones we're interested in. This forms "filtered views" on top of streams.
        filter_subject: Option<String>,

        /// p2panda topic used to exchange the "filtered" NATS stream with external nodes.
        ///
        /// While this is not strictly required for NATS JetStream we keep it here to inform other
        /// parts of rhio about which topic is used for this stream.
        topic: TopicId,

        /// Channel to receive all messages (old and new) from this subscription, including
        /// errors and "readiness" state.
        ///
        /// An initial downloading of all persisted data from the NATS server is required when
        /// starting to subscribe to a subject. The channel will eventually send an event to the
        /// user to signal when the initialization has finished.
        reply: oneshot::Sender<Result<broadcast::Receiver<JetStreamEvent>>>,
    },
    Shutdown {
        reply: oneshot::Sender<()>,
    },
}

pub struct NatsActor {
    inbox: mpsc::Receiver<ToNatsActor>,
    nats_jetstream: JetstreamContext,
    consumers: HashMap<ConsumerId, Consumer>,
}

impl NatsActor {
    pub fn new(nats_client: NatsClient, inbox: mpsc::Receiver<ToNatsActor>) -> Self {
        let nats_jetstream = async_nats::jetstream::new(nats_client.clone());

        Self {
            inbox,
            nats_jetstream,
            consumers: HashMap::new(),
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
                payload,
                reply,
            } => {
                let result = self.on_publish(wait_for_ack, subject, payload).await;
                reply.send(result).ok();
            }
            ToNatsActor::Subscribe {
                stream_name,
                filter_subject,
                topic,
                reply,
            } => {
                let result = self.on_subscribe(stream_name, filter_subject, topic).await;
                reply.send(result).ok();
            }
            ToNatsActor::Shutdown { .. } => {
                unreachable!("handled in run_inner");
            }
        }

        Ok(())
    }

    /// Publish a message inside an existing stream.
    ///
    /// This method fails if the stream does not exist on the NATS server
    async fn on_publish(
        &self,
        wait_for_ack: bool,
        subject: Subject,
        payload: Vec<u8>,
    ) -> Result<()> {
        let server_ack = self.nats_jetstream.publish(subject, payload.into()).await?;

        // Wait until the server confirmed receiving this message, to make sure it got delivered
        // and persisted
        if wait_for_ack {
            server_ack.await.context("publish message to nats server")?;
        }

        Ok(())
    }

    async fn on_subscribe(
        &mut self,
        stream_name: String,
        filter_subject: Option<String>,
        topic: TopicId,
    ) -> Result<broadcast::Receiver<JetStreamEvent>> {
        let consumer_id = ConsumerId::new(stream_name.clone(), filter_subject.clone());
        if self.consumers.contains_key(&consumer_id) {
            bail!(
                "consumer for stream '{}' with filter subject '{}' is already registered",
                stream_name,
                filter_subject.unwrap_or_default()
            );
        }

        let mut consumer =
            Consumer::new(&self.nats_jetstream, stream_name, filter_subject, topic).await?;
        let rx = consumer.subscribe();

        self.consumers.insert(consumer_id, consumer);

        Ok(rx)
    }

    async fn shutdown(&mut self) -> Result<()> {
        Ok(())
    }
}
