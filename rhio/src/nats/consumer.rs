use anyhow::{Context, Result};
use async_nats::error::Error;
use async_nats::jetstream::consumer::push::{
    Config as ConsumerConfig, Messages, MessagesErrorKind,
};
use async_nats::jetstream::consumer::{AckPolicy, DeliverPolicy, PushConsumer};
use async_nats::jetstream::{Context as JetstreamContext, Message as MessageWithContext};
use async_nats::Message as NatsMessage;
use futures_util::future::{MapErr, Shared};
use futures_util::{FutureExt, TryFutureExt};
use rand::random;
use rhio_core::ScopedSubject;
use tokio::sync::broadcast;
use tokio::task::JoinError;
use tokio_stream::StreamExt;
use tokio_util::task::AbortOnDropHandle;
use tracing::{error, span, trace, Level, Span};

use crate::JoinErrToStr;

pub type StreamName = String;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct ConsumerId(StreamName, String);

impl ConsumerId {
    pub fn new(stream_name: String, filter_subject: String) -> Self {
        Self(stream_name, filter_subject)
    }
}

#[derive(Debug, Clone)]
pub enum JetStreamEvent {
    /// Finished downloading all _past_ messages from NATS JetStream consumer.
    ///
    /// This event is especially used when setting the Delivery Policy to "All".
    InitCompleted {
        #[allow(dead_code)]
        topic_id: [u8; 32],
    },

    /// Received a message from NATS JetStream consumer.
    ///
    /// This includes both past and new messages.
    Message {
        is_init: bool,
        message: NatsMessage,
        topic_id: [u8; 32],
    },

    /// NATS JetStream consumer failed.
    Failed {
        stream_name: StreamName,
        reason: String,
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
    messages: Messages,
    num_pending: u64,
    status: ConsumerStatus,
    stream_name: StreamName,
    topic_id: [u8; 32],
    _span: Span,
}

impl ConsumerActor {
    pub fn new(
        subscribers_tx: broadcast::Sender<JetStreamEvent>,
        messages: Messages,
        num_pending: u64,
        stream_name: StreamName,
        topic_id: [u8; 32],
        _span: Span,
    ) -> Self {
        Self {
            subscribers_tx,
            messages,
            num_pending,
            status: ConsumerStatus::Initializing,
            stream_name,
            topic_id,
            _span,
        }
    }

    pub async fn run(mut self) -> Result<()> {
        trace!(parent: &self._span, "start consumer");
        let result = self.run_inner().await;
        drop(self);
        result
    }

    async fn run_inner(&mut self) -> Result<()> {
        // Do not wait for incoming past messages during initialization if there is none.
        if self.num_pending == 0 {
            self.on_init_complete()?;
        }

        loop {
            match self.messages.next().await {
                Some(message) => {
                    if let Err(err) = self.on_message(message).await {
                        break Err(err);
                    }
                }
                None => break Ok(()),
            }
        }
    }

    async fn on_message(
        &mut self,
        message: Result<MessageWithContext, Error<MessagesErrorKind>>,
    ) -> Result<()> {
        if let Err(err) = self.on_message_inner(message).await {
            error!(parent: &self._span, "consuming nats stream failed: {err}");

            self.subscribers_tx.send(JetStreamEvent::Failed {
                stream_name: self.stream_name.clone(),
                reason: err.to_string(),
            })?;
            self.status = ConsumerStatus::Failed;

            Err(err)
        } else {
            Ok(())
        }
    }

    async fn on_message_inner(
        &mut self,
        message: Result<MessageWithContext, Error<MessagesErrorKind>>,
    ) -> Result<()> {
        let message = message?;

        self.subscribers_tx.send(JetStreamEvent::Message {
            is_init: matches!(self.status, ConsumerStatus::Initializing),
            message: message.message.clone(),
            topic_id: self.topic_id,
        })?;

        if matches!(self.status, ConsumerStatus::Initializing) {
            self.num_pending -= 1;
            trace!(parent: &self._span, payload = ?message.payload, num_pending = self.num_pending, is_init = true, "message");
            if self.num_pending == 0 {
                self.on_init_complete()?;
            }
        } else {
            trace!(parent: &self._span, payload = ?message.payload, "message");
        }

        Ok(())
    }

    fn on_init_complete(&mut self) -> Result<()> {
        trace!(parent: &self._span, "completed initialization phase");
        self.status = ConsumerStatus::Streaming;
        self.subscribers_tx.send(JetStreamEvent::InitCompleted {
            topic_id: self.topic_id,
        })?;
        Ok(())
    }
}

impl Drop for ConsumerActor {
    fn drop(&mut self) {
        trace!(parent: &self._span, "drop consumer");
    }
}

#[derive(Debug, Clone)]
pub struct Consumer {
    subscribers_tx: broadcast::Sender<JetStreamEvent>,
    #[allow(dead_code)]
    actor_handle: Shared<MapErr<AbortOnDropHandle<()>, JoinErrToStr>>,
}

impl Consumer {
    /// Create a consumer of a NATS stream.
    ///
    /// The consumers used here are push-based, "un-acking" and ephemeral, meaning that no state of
    /// the consumer is persisted on the NATS server and no message is marked as "read" to be able
    /// to re-play them again when the process restarts.
    ///
    /// Since NATS streams are also used for persistance with their own wide range of limit
    /// configurations, rhio does not create any streams automatically but merely consumes them.
    /// This allows rhio operators to have full flexibility over the nature of the stream. This is
    /// why for every published subject a "stream name" needs to be mentioned.
    pub async fn new(
        context: &JetstreamContext,
        stream_name: StreamName,
        filter_subject: ScopedSubject,
        deliver_policy: DeliverPolicy,
        topic_id: [u8; 32],
    ) -> Result<Self> {
        let mut consumer: PushConsumer = context
            // Streams need to already be created on the server, if not, this method will fail
            // here. Note that no checks are applied here for validating if the NATS stream
            // configuration is compatible with rhio's design.
            .get_stream(&stream_name)
            .await
            .context(format!(
                "create or get '{}' stream from nats server",
                stream_name,
            ))?
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
                deliver_subject: {
                    // @TODO: Add a note here
                    let random_deliver_subject: u32 = random();
                    format!("rhio-{random_deliver_subject}")
                },
                // For rhio two different delivery policies are configured:
                //
                // 1. Live-Mode: We're only interested in _upcoming_ messages as this consumer will
                //    only be used to forward NATS messages into the gossip overlay. This happens
                //    when a rhio node decided to "publish" a NATS subject, the created consumer
                //    lives as long as the process.
                // 2. Sync-Session: Here we want to load and exchange _past_ messages, usually
                //    loading all messages from after a given timestamp. This happens when a remote
                //    rhio node requests data from a NATS subject from us, the created consumer
                //    lives as long as the sync session with this remote peer.
                deliver_policy,
                // We filter the given stream based on this subject filter, like this we can have
                // different "views" on the same stream.
                filter_subject: filter_subject.to_string(),
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
                stream_name,
            ))?;

        // Retrieve info about the consumer to learn how many messages are currently persisted on
        // the server (number of "pending messages"). These are the messages we need to download
        // first before we can continue.
        let consumer_info = consumer.info().await?;
        let consumer_name = consumer_info.name.clone();
        let num_pending = consumer_info.num_pending;

        let messages = consumer.messages().await.context("get message stream")?;

        let (subscribers_tx, _) = broadcast::channel(256);

        let span = span!(Level::TRACE, "consumer", id = %consumer_name);
        let deliver_policy_str = match deliver_policy {
            DeliverPolicy::All => "all",
            DeliverPolicy::New => "new",
            _ => unimplemented!(),
        };
        trace!(
            parent: &span,
            stream = %stream_name,
            subject = %filter_subject,
            deliver_policy = deliver_policy_str,
            num_pending = num_pending,
            "create consumer for NATS"
        );

        let consumer_actor = ConsumerActor::new(
            subscribers_tx.clone(),
            messages,
            num_pending,
            stream_name,
            topic_id,
            span,
        );

        let actor_handle = tokio::task::spawn(async move {
            if let Err(err) = consumer_actor.run().await {
                error!("consumer actor failed: {err:?}");
            }
        });

        let actor_drop_handle = AbortOnDropHandle::new(actor_handle)
            .map_err(Box::new(|e: JoinError| e.to_string()) as JoinErrToStr)
            .shared();

        let consumer = Self {
            subscribers_tx,
            actor_handle: actor_drop_handle,
        };

        Ok(consumer)
    }

    pub fn subscribe(&mut self) -> broadcast::Receiver<JetStreamEvent> {
        self.subscribers_tx.subscribe()
    }
}
