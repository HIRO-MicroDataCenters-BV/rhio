use anyhow::{anyhow, Result};
use async_nats::jetstream::consumer::{DeliverPolicy, Info};
use async_nats::HeaderMap;
use async_nats::Message;
use async_trait::async_trait;
use bytes::Bytes;
use futures::Stream;
use rhio_core::Subject;

use crate::StreamName;

/// Represents a stream of NATS JetStream messages.
///
/// This trait is used to define a stream of messages that can be consumed from a NATS JetStream consumer.
/// The stream yields `Result<Message, MessagesError>` items.
pub trait NatsMessageStream: Stream<Item = NatsStreamProtocol> + Sized {}

/// A trait for a NATS client that interacts with NATS JetStream.
///
/// This trait defines the necessary methods for creating a consumer stream and publishing messages to NATS.
///
/// # Type Parameters
///
/// * `M` - A type that implements the `NatsMessageStream` trait.
///
/// # Methods
///
/// * `create_consumer_stream` - Creates a consumer stream for a given stream name, filter subjects, and delivery policy.
/// * `publish` - Publishes a message to a given subject with an optional payload and headers.
#[async_trait]
pub trait NatsClient<M: NatsMessageStream>: Sized {
    /// Creates a consumer stream for a given stream name, filter subjects, and delivery policy.
    ///
    /// # Arguments
    ///
    /// * `stream_name` - The name of the stream to consume from.
    /// * `filter_subjects` - A vector of subjects to filter the messages.
    /// * `deliver_policy` - The delivery policy for the consumer.
    ///
    /// # Returns
    ///
    /// A tuple containing the consumer stream and its information.
    async fn create_consumer_stream(
        &self,
        consumer_name: String,
        stream_name: StreamName,
        filter_subjects: Vec<Subject>,
        deliver_policy: DeliverPolicy,
    ) -> Result<(M, Info)>;

    /// Publishes a message to a given subject with an optional payload and headers.
    ///
    /// # Arguments
    ///
    /// * `wait_for_ack` - Whether to wait for an acknowledgment from the server.
    /// * `subject` - The subject to publish the message to.
    /// * `payload` - The payload of the message.
    /// * `headers` - Optional headers to include with the message.
    ///
    /// # Returns
    ///
    /// A result indicating success or failure.
    async fn publish(
        &self,
        wait_for_ack: bool,
        subject: String,
        payload: Bytes,
        headers: Option<HeaderMap>,
    ) -> Result<()>;
}

#[derive(Clone, Debug)]
pub enum NatsStreamProtocol {
    Msg { msg: Message, seq: Option<u64> },
    Error { msg: String },
    ServerDisconnect,
}

impl From<NatsStreamProtocol> for Result<Message> {
    fn from(value: NatsStreamProtocol) -> Self {
        match value {
            NatsStreamProtocol::Msg { msg, .. } => Ok(msg),
            NatsStreamProtocol::ServerDisconnect => {
                Err(anyhow!("NatsStreamProtocol::ServerDisconnect"))
            }
            NatsStreamProtocol::Error { msg } => Err(anyhow!("NatsStreamProtocol::Error {msg}")),
        }
    }
}
