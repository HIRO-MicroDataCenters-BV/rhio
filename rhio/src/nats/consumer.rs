use std::sync::Arc;

use crate::nats::Subject;
use crate::utils::nats::error::RetryConfig;
use crate::utils::nats::factory::NatsStreamFactory;
use crate::utils::nats::stream::RecoverableNatsStreamImpl;
use anyhow::Context;
use anyhow::Result;
use async_nats::jetstream::consumer::DeliverPolicy;
use async_nats::Message as NatsMessage;
use futures_util::future::{MapErr, Shared};
use futures_util::{FutureExt, TryFutureExt};
use tokio::task::JoinError;
use tokio_stream::StreamExt;
use tokio_util::task::AbortOnDropHandle;
use tracing::{error, info, span, trace, Level, Span};

use crate::JoinErrToStr;

use super::client::types::NatsClient;
use super::client::types::NatsMessageStream;
use super::client::types::NatsStreamProtocol;

pub type StreamName = String;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct ConsumerId(StreamName, String);

impl ConsumerId {
    pub fn new(stream_name: String, filter_subject: String) -> Self {
        Self(stream_name, filter_subject)
    }
}

impl std::fmt::Display for ConsumerId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}-{}", self.0, self.1)
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
pub struct ConsumerActor<M>
where
    M: NatsMessageStream + Unpin,
{
    subscribers_tx: loole::Sender<JetStreamEvent>,
    messages: M,
    num_pending: u64,
    status: ConsumerStatus,
    stream_name: StreamName,
    topic_id: [u8; 32],
    span: Span,
}

impl<M> ConsumerActor<M>
where
    M: NatsMessageStream + Unpin,
{
    pub fn new(
        subscribers_tx: loole::Sender<JetStreamEvent>,
        messages: M,
        num_pending: u64,
        stream_name: StreamName,
        topic_id: [u8; 32],
        span: Span,
    ) -> Self {
        Self {
            subscribers_tx,
            messages,
            num_pending,
            status: ConsumerStatus::Initializing,
            stream_name,
            topic_id,
            span,
        }
    }

    pub async fn run(mut self) -> Result<()> {
        trace!(parent: &self.span, "start consumer");
        let result = self.run_inner().await;
        drop(self);
        result
    }

    async fn run_inner(&mut self) -> Result<()> {
        // Do not wait for incoming past messages during initialization if there is none.
        if self.num_pending == 0 {
            self.on_init_complete()?;
        }

        let inner_result = loop {
            match self.messages.next().await {
                Some(message) => {
                    if let Err(err) = self.on_message(message).await {
                        break Err(err);
                    }
                }
                None => {
                    info!("stream consumed all messages - {}", self.stream_name);
                    break Ok(());
                }
            }
        };
        trace!(parent: &self.span, "consumer stopped, result={:?}", inner_result);
        inner_result
    }

    async fn on_message(&mut self, message: NatsStreamProtocol) -> Result<()> {
        if let Err(err) = self.on_message_inner(message).await {
            error!(parent: &self.span, "consuming nats stream failed: {err}");

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

    async fn on_message_inner(&mut self, protocol_message: NatsStreamProtocol) -> Result<()> {
        let message: Result<async_nats::Message> = protocol_message.into();
        let message = message?;

        self.subscribers_tx
            .send(JetStreamEvent::Message {
                is_init: matches!(self.status, ConsumerStatus::Initializing),
                message: message.clone(),
                topic_id: self.topic_id,
            })
            .context("sending JetStreamEvent::Message")?;

        if matches!(self.status, ConsumerStatus::Initializing) {
            self.num_pending -= 1;
            trace!(parent: &self.span, payload = ?message.payload, num_pending = self.num_pending, is_init = true, "message");
            if self.num_pending == 0 {
                self.on_init_complete()?;
            }
        } else {
            trace!(parent: &self.span, payload = ?message.payload, "message");
        }

        Ok(())
    }

    fn on_init_complete(&mut self) -> Result<()> {
        trace!(parent: &self.span, "completed initialization phase");
        self.status = ConsumerStatus::Streaming;
        self.subscribers_tx
            .send(JetStreamEvent::InitCompleted {
                topic_id: self.topic_id,
            })
            .context("sending JetStreamEvent::InitCompleted")?;
        Ok(())
    }
}

impl<M> Drop for ConsumerActor<M>
where
    M: NatsMessageStream + Unpin,
{
    fn drop(&mut self) {
        trace!(parent: &self.span, "drop consumer");
    }
}

#[derive(Debug, Clone)]
pub struct Consumer {
    #[allow(dead_code)]
    subscribers_tx: loole::Sender<JetStreamEvent>,
    subscribers_rx: loole::Receiver<JetStreamEvent>,
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
    pub async fn new<M, N>(
        client: Arc<N>,
        consumer_id: ConsumerId,
        deliver_policy: DeliverPolicy,
        filter_subjects: Vec<Subject>,
        stream_name: StreamName,
        topic_id: [u8; 32],
    ) -> Result<Self>
    where
        M: NatsMessageStream + Send + Sync + Unpin + 'static,
        N: NatsClient<M> + 'static + Send + Sync,
    {
        let consumer_name = consumer_id.to_string();
        let span = span!(Level::TRACE, "consumer", id = %consumer_name);

        let (subscribers_tx, subscribers_rx) = loole::bounded(256);
        let factory = NatsStreamFactory::new(
            client,
            consumer_id.clone(),
            stream_name.clone(),
            filter_subjects,
            deliver_policy,
            span.clone(),
        );
        let retry_options = RetryConfig::default();
        let messages = RecoverableNatsStreamImpl::new(factory, retry_options, span.clone());

        // We are supposed to get num_pending from consumer_info.
        // It is not completely clear why we need to skip num_pending messages later in consumer_actor
        // Konstantin: setting this to 0 until the use case is clear
        let num_pending = 0;

        let consumer_actor = ConsumerActor::new(
            subscribers_tx.clone(),
            messages,
            num_pending,
            stream_name,
            topic_id,
            span.clone(),
        );

        let actor_handle = tokio::task::spawn(async move {
            if let Err(err) = consumer_actor.run().await {
                error!(parent: &span, "consumer actor failed: {err:?}");
            }
        });

        let actor_drop_handle = AbortOnDropHandle::new(actor_handle)
            .map_err(Box::new(|e: JoinError| e.to_string()) as JoinErrToStr)
            .shared();

        Ok(Self {
            subscribers_tx,
            subscribers_rx,
            actor_handle: actor_drop_handle,
        })
    }

    pub fn subscribe(&mut self) -> loole::Receiver<JetStreamEvent> {
        self.subscribers_rx.clone()
    }
}
