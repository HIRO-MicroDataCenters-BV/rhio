use anyhow::{anyhow, Context, Result};
use async_nats::error::Error;
use async_nats::jetstream::consumer::push::{
    Config as ConsumerConfig, Messages, MessagesErrorKind,
};
use async_nats::jetstream::consumer::{AckPolicy, PushConsumer};
use async_nats::jetstream::{Context as JetstreamContext, Message};
use p2panda_net::{SharedAbortingJoinHandle, ToBytes};
use rhio_core::{Subject, TopicId};
use tokio::sync::{broadcast, mpsc};
use tokio_stream::StreamExt;
use tracing::error;

pub type StreamName = String;

pub type FilterSubject = Option<Subject>;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct ConsumerId(StreamName, FilterSubject);

impl ConsumerId {
    pub fn new(stream_name: String, filter_subject: Option<String>) -> Self {
        Self(stream_name, filter_subject)
    }
}

pub enum ToConsumerActor {}

#[derive(Debug, Clone)]
pub enum JetStreamEvent {
    InitCompleted {
        topic: TopicId,
    },
    InitFailed {
        stream_name: StreamName,
        reason: String,
    },
    StreamFailed {
        stream_name: StreamName,
        reason: String,
    },
    Message {
        payload: Vec<u8>,
        topic: TopicId,
    },
}

#[derive(Debug, PartialEq)]
enum ConsumerStatus {
    Initializing,
    Streaming,
    Failed,
}

/// Manages a NATS JetStream consumer.
///
/// A consumer is a stateful view of a stream. It acts as an interface for clients to consume a
/// subset of messages stored in a stream.
///
/// Streams are message stores in NATS JetStream, each stream defines how messages are stored and
/// what the limits (duration, size, interest) of the retention are. In rhio we use streams for
/// permament storage: messages are kept forever (for now). Streams consume normal NATS subjects,
/// any message published on those subjects will be captured in the defined storage system.
pub struct ConsumerActor {
    subscribers_tx: broadcast::Sender<JetStreamEvent>,
    #[allow(dead_code)]
    inbox: mpsc::Receiver<ToConsumerActor>,
    messages: Messages,
    initial_stream_height: u64,
    status: ConsumerStatus,
    stream_name: StreamName,
    topic: TopicId,
}

impl ConsumerActor {
    pub fn new(
        subscribers_tx: broadcast::Sender<JetStreamEvent>,
        inbox: mpsc::Receiver<ToConsumerActor>,
        messages: Messages,
        initial_stream_height: u64,
        stream_name: StreamName,
        topic: TopicId,
    ) -> Self {
        Self {
            subscribers_tx,
            inbox,
            messages,
            initial_stream_height,
            status: ConsumerStatus::Initializing,
            stream_name,
            topic,
        }
    }

    pub async fn run(mut self) -> Result<()> {
        let result = self.run_inner().await;
        drop(self);
        result
    }

    async fn run_inner(&mut self) -> Result<()> {
        loop {
            tokio::select! {
                biased;
                Some(message) = self.messages.next() => {
                    if let Err(err) = self.on_message(message).await {
                        break Err(err);
                    }
                },
            }
        }
    }

    async fn on_message(
        &mut self,
        message: Result<Message, Error<MessagesErrorKind>>,
    ) -> Result<()> {
        if let Err(err) = self.on_message_inner(message).await {
            match self.status {
                ConsumerStatus::Initializing => {
                    self.subscribers_tx.send(JetStreamEvent::InitFailed {
                        stream_name: self.stream_name.clone(),
                        reason: err.to_string(),
                    })?;
                }
                ConsumerStatus::Streaming => {
                    self.subscribers_tx.send(JetStreamEvent::StreamFailed {
                        stream_name: self.stream_name.clone(),
                        reason: err.to_string(),
                    })?;
                }
                ConsumerStatus::Failed => (),
            }

            self.status = ConsumerStatus::Failed;
        }

        Ok(())
    }

    async fn on_message_inner(
        &mut self,
        message: Result<Message, Error<MessagesErrorKind>>,
    ) -> Result<()> {
        let message = message?;

        self.subscribers_tx.send(JetStreamEvent::Message {
            payload: message.payload.to_bytes(),
            topic: self.topic,
        })?;

        if matches!(self.status, ConsumerStatus::Initializing) {
            let info = message.info().map_err(|err| anyhow!(err))?;
            if info.stream_sequence >= self.initial_stream_height {
                self.status = ConsumerStatus::Streaming;
                self.subscribers_tx
                    .send(JetStreamEvent::InitCompleted { topic: self.topic })?;
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct Consumer {
    subscribers_tx: broadcast::Sender<JetStreamEvent>,
    #[allow(dead_code)]
    consumer_actor_tx: mpsc::Sender<ToConsumerActor>,
    #[allow(dead_code)]
    actor_handle: SharedAbortingJoinHandle<()>,
}

impl Consumer {
    /// Create a consumer of a NATS stream.
    ///
    /// This method launches an async task and does the following work:
    ///
    /// 1. Create and connect consumer to existing NATS JetStream "stream" identified by name and
    ///    optionally filtered with an "subject filter"
    /// 2. Download all past (filtered) messages, which have been persisted in this stream
    /// 3. Finally, continue streaming future messages
    ///
    /// The consumers used here are push-based, "un-acking" and ephemeral, meaning that no state of
    /// the consumer is persisted on the NATS server and no message is marked as "read" to be able
    /// to re-play them again when the process restarts.
    pub async fn new(
        context: &JetstreamContext,
        stream_name: StreamName,
        filter_subject: FilterSubject,
        topic: TopicId,
    ) -> Result<Self> {
        let mut consumer: PushConsumer = context
            // Streams need to already be created on the server, if not, this method will fail
            // here. Note that no checks are applied here for validating if the NATS stream
            // configuration is compatible with rhio's design
            .get_stream(&stream_name)
            .await
            .context(format!("get '{}' stream from NATS server", stream_name))?
            .create_consumer(ConsumerConfig {
                // Setting a delivery subject is crucial for making this consumer push-based. We
                // need to create a push based consumer as pull-based ones are required to
                // explicitly acknowledge messages.
                //
                // @NOTE(adz): Unclear to me what this really does other than it is required to be
                // set for push-consumers? The documentation says: "The subject to deliver messages
                // to. Setting this field decides whether the consumer is push or pull-based. With
                // a deliver subject, the server will push messages to clients subscribed to this
                // subject." https://docs.nats.io/nats-concepts/jetstream/consumers#push-specific
                //
                // .. it seems to not matter what the value inside this field is, we will still
                // receive all messages from that stream, optionally filtered by "filter_subject"?
                deliver_subject: "rhio".to_string(),
                // We can optionally filter the given stream based on this subject filter, like
                // this we can have different "views" on the same stream.
                filter_subject: filter_subject.clone().unwrap_or_default(),
                // This is an ephemeral consumer which will not be persisted on the server / the
                // progress of the consumer will not be remembered. We do this by _not_ setting
                // "durable_name".
                durable_name: None,
                // Do _not_ acknowledge every incoming message, as we want to receive them _again_
                // after rhio got restarted. The to-be-consumed stream needs to accommodate for
                // this setting and accept an unlimited amount of un-acked message deliveries.
                ack_policy: AckPolicy::None,
                ..Default::default()
            })
            .await
            .context(format!(
                "create ephemeral jetstream consumer for '{}' stream",
                stream_name
            ))?;

        // Retreive info about the consumer to learn how many messages are currently persisted on
        // the server (number of "pending messages") and we need to download first
        let initial_stream_height = {
            let consumer_info = consumer.info().await?;
            consumer_info.num_pending + 1 // sequence numbers start with 1 in NATS JetStream
        };

        let messages = consumer.messages().await.context("get message stream")?;

        let (consumer_actor_tx, consumer_actor_rx) = mpsc::channel(64);
        let (subscribers_tx, _) = broadcast::channel(256);

        let consumer_actor = ConsumerActor::new(
            subscribers_tx.clone(),
            consumer_actor_rx,
            messages,
            initial_stream_height,
            stream_name,
            topic,
        );

        let actor_handle = tokio::task::spawn(async move {
            if let Err(err) = consumer_actor.run().await {
                error!("consumer actor failed: {err:?}");
            }
        });

        let consumer = Self {
            subscribers_tx,
            consumer_actor_tx,
            actor_handle: actor_handle.into(),
        };

        Ok(consumer)
    }

    pub fn subscribe(&mut self) -> broadcast::Receiver<JetStreamEvent> {
        self.subscribers_tx.subscribe()
    }
}
