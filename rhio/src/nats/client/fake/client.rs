use crate::nats::HeaderMap;
use crate::nats::client::fake::server::MessageStream;
use crate::nats::client::fake::server::TEST_FAKE_SERVER;
use crate::nats::client::types::NatsStreamProtocol;
use anyhow::{Context as AnyhowContext, Result};
use async_nats::Message;
use async_nats::jetstream::consumer::DeliverPolicy;
use async_nats::jetstream::consumer::{Info, SequenceInfo};
use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use futures::stream::SelectAll;
use pin_project::pin_project;
use pin_project::pinned_drop;
use rand::random;
use rhio_config::configuration::NatsConfig;
use rhio_core::Subject;
use s3::creds::time::OffsetDateTime;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use tokio::pin;
use tracing::info;

use crate::StreamName;

use super::super::types::{NatsClient, NatsMessageStream};
use super::server::{FakeNatsServer, FakeSubscription};

/// `FakeNatsClient` is a mock implementation of a NATS client used for testing purposes.
/// It interacts with a `FakeNatsServer` to simulate NATS server behavior.
///
/// # Fields
/// - `client_id`: A unique identifier for the client.
/// - `server`: A reference-counted pointer to the `FakeNatsServer`.
///
/// # Methods
/// - `new(config: NatsConfig) -> Result<Self>`:
///   Creates a new `FakeNatsClient` instance with a unique client ID and associates it with a `FakeNatsServer`.
///
/// # Trait Implementations
/// Implements the `NatsClient` trait for `FakeNatsClient`:
/// - `create_consumer_stream(&self, stream_name: StreamName, filter_subjects: Vec<Subject>, deliver_policy: DeliverPolicy) -> Result<(FakeNatsMessages, Info)>`:
///   Creates a consumer stream with the specified parameters and returns a `FakeNatsMessages` stream and consumer info.
///
/// - `publish(&self, _wait_for_ack: bool, subject: String, payload: Bytes, headers: Option<HeaderMap>) -> Result<()>`:
///   Publishes a message to the specified subject with optional headers.
pub struct FakeNatsClient {
    client_id: String,
    server: Arc<FakeNatsServer>,
}

impl FakeNatsClient {
    pub fn new(config: NatsConfig) -> Result<Self> {
        let client_id = format!("rhio-{}", random::<u64>().to_string());
        info!("creating client {client_id} for config {config:?}");

        let server = TEST_FAKE_SERVER
            .entry(config)
            .or_insert_with(|| Arc::new(FakeNatsServer::new()))
            .value()
            .clone();
        Ok(FakeNatsClient { client_id, server })
    }

    pub async fn publish_test_messages(&self, subject: &String, ids: &Vec<usize>) -> Result<()> {
        for i in ids {
            let payload = format!("message {}", i);
            self.publish(
                false,
                subject.clone().into(),
                Bytes::copy_from_slice(payload.as_bytes()),
                None,
            )
            .await?;
        }
        Ok(())
    }
}

#[async_trait]
impl NatsClient<FakeNatsMessages> for FakeNatsClient {
    async fn create_consumer_stream(
        &self,
        consumer_name: String,
        stream_name: StreamName,
        filter_subjects: Vec<Subject>,
        deliver_policy: DeliverPolicy,
    ) -> Result<(FakeNatsMessages, Info)> {
        let (subscription, messages) = self
            .server
            .add_subscription(
                self.client_id.clone(),
                consumer_name,
                filter_subjects,
                deliver_policy,
            )
            .await
            .context("FakeNatsClient: create consumer stream")?;

        let info = to_fake_consumer_info(&self.client_id, &stream_name);
        Ok((
            FakeNatsMessages {
                messages,
                server: self.server.clone(),
                client_id: self.client_id.clone(),
                subscription,
            },
            info,
        ))
    }

    async fn publish(
        &self,
        _wait_for_ack: bool,
        subject: String,
        payload: Bytes,
        headers: Option<HeaderMap>,
    ) -> Result<()> {
        let message = to_nats_message(subject.clone(), payload, headers);
        let subject = Subject::from_str(&subject)?;

        self.server
            .publish(subject, message)
            .await
            .context("FakeNatsClient: publish message to server")?;

        Ok(())
    }
}

#[pin_project(PinnedDrop)]
pub struct FakeNatsMessages {
    #[pin]
    messages: SelectAll<Box<MessageStream>>,
    server: Arc<FakeNatsServer>,
    client_id: String,
    subscription: FakeSubscription,
}

impl NatsMessageStream for FakeNatsMessages {}

impl futures::Stream for FakeNatsMessages {
    type Item = NatsStreamProtocol;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.project();
        this.messages.poll_next(cx)
    }
}

#[pinned_drop]
impl PinnedDrop for FakeNatsMessages {
    fn drop(self: Pin<&mut Self>) {
        let this = self.project();
        this.server
            .remove_subscription(this.client_id, &this.subscription);
    }
}

/// `Consumer` is a struct that wraps around a stream of NATS messages and provides handy methods to receive messages with a timeout.
///
/// # Type Parameters
/// - `M`: A type that implements the `NatsMessageStream` trait, representing a stream of NATS messages.
///
/// # Fields
/// - `messages`: A pinned boxed stream of NATS messages.
///
/// # Methods
/// - `new(messages: M) -> Consumer<M>`:
///   Creates a new `Consumer` instance with the provided message stream.
///
/// - `recv_timeout(&mut self, timeout: std::time::Duration, count: usize) -> Result<Vec<Message>>`:
///   Receives a specified number of messages from the stream within a given timeout duration. If the timeout is reached before the specified number of messages are received, an error is returned.
///
/// - `recv_count(&mut self, count: usize) -> Result<Vec<Message>>`:
///   Receives a specified number of messages from the stream. This method is used internally by `recv_timeout`.
///
/// # Example
/// ```rust
/// use std::time::Duration;
/// use async_nats::Message;
/// use anyhow::Result;
///
/// async fn example<M: NatsMessageStream>(mut consumer: Consumer<M>) -> Result<()> {
///     let messages: Vec<Message> = consumer.recv_timeout(Duration::from_secs(5), 10).await?;
///     for message in messages {
///         println!("Received message: {:?}", message);
///     }
///     Ok(())
/// }
/// ```
pub struct Consumer<M: NatsMessageStream> {
    messages: Pin<Box<M>>,
}

impl<M: NatsMessageStream> Consumer<M> {
    pub fn new(messages: M) -> Consumer<M> {
        Consumer {
            messages: Box::pin(messages),
        }
    }

    pub async fn recv_timeout(
        &mut self,
        timeout: std::time::Duration,
        count: usize,
    ) -> Result<Vec<Message>> {
        tokio::time::timeout(timeout, async { self.recv_count(count).await }).await?
    }

    async fn recv_count(&mut self, count: usize) -> Result<Vec<Message>> {
        let mut result = vec![];
        while let Some(maybe_message) = self.messages.next().await {
            let maybe_message: Result<Message> = maybe_message.into();
            let message =
                maybe_message.context("Consumer: receiving message from fake message stream")?;
            result.push(message);
            if result.len() >= count {
                break;
            }
        }

        Ok(result)
    }
}

fn to_fake_consumer_info(client_id: &String, stream_name: &String) -> Info {
    Info {
        stream_name: stream_name.clone(),
        name: client_id.clone(),
        created: OffsetDateTime::now_utc(),
        config: Default::default(),
        delivered: SequenceInfo {
            consumer_sequence: 0,
            stream_sequence: 0,
            last_active: None,
        },
        ack_floor: SequenceInfo {
            consumer_sequence: 0,
            stream_sequence: 0,
            last_active: None,
        },
        num_ack_pending: 0,
        num_redelivered: 0,
        num_waiting: 0,
        num_pending: 0,
        cluster: None,
        push_bound: false,
        paused: false,
        pause_remaining: None,
    }
}

fn to_nats_message(subject: String, data: Bytes, headers: Option<HeaderMap>) -> Message {
    Message {
        subject: async_nats::Subject::from(subject.clone()),
        reply: None,
        payload: data,
        headers,
        status: None,
        description: None,
        length: 0,
    }
}

pub fn test_consumer() -> String {
    format!("test-consumer-{}", random::<u16>())
}
