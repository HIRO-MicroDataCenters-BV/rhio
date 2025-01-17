use crate::nats::HeaderMap;
use anyhow::Result;
use async_nats::jetstream::consumer::DeliverPolicy;
use async_nats::Message;
use bytes::Bytes;
use rhio_core::Subject;
use std::marker::PhantomData;
use std::sync::Arc;
use tokio::runtime::Runtime;

use crate::StreamName;

use super::{
    super::types::{NatsClient, NatsMessageStream},
    client::Consumer,
};

/// A blocking client for interacting with NATS.
///
/// This client wraps an asynchronous NATS client and provides blocking
/// methods for publishing messages and creating consumers.
///
/// # Type Parameters
///
/// * `T` - The type of the NATS client.
/// * `M` - The type of the NATS message stream.
///
/// # Methods
///
/// * `new(inner: T, runtime: Arc<Runtime>) -> Self`
///   - Creates a new `BlockingClient`.
///
/// * `publish(&self, subject: String, payload: Bytes, headers: Option<HeaderMap>) -> Result<()>`
///   - Publishes a message to the given subject with the specified payload and headers.
///
/// * `create_consumer(&self, stream_name: StreamName, filter_subjects: Vec<Subject>, _deliver_policy: DeliverPolicy) -> Result<BlockingConsumer<M>>`
///   - Creates a new consumer for the specified stream and subjects with the given delivery policy.
///
/// A blocking consumer for receiving messages from NATS.
///
/// This consumer wraps an asynchronous consumer and provides a blocking
/// method for receiving messages.
///
/// # Type Parameters
///
/// * `M` - The type of the NATS message stream.
///
/// # Fields
///
/// * `consumer` - The inner asynchronous consumer.
/// * `runtime` - The Tokio runtime used for blocking operations.
///
/// # Methods
///
/// * `new(consumer: Consumer<M>, runtime: Arc<Runtime>) -> BlockingConsumer<M>`
///   - Creates a new `BlockingConsumer`.
///
/// * `recv_count(&mut self, timeout: std::time::Duration, count: usize) -> Result<Vec<Message>>`
///   - Receives a specified number of messages with a timeout.
pub struct BlockingClient<T, M>
where
    T: NatsClient<M>,
    M: NatsMessageStream,
{
    /// The inner asynchronous NATS client.
    inner: T,
    /// The Tokio runtime used for blocking operations.
    runtime: Arc<Runtime>,
    phantom: PhantomData<M>,
}

impl<T, M> BlockingClient<T, M>
where
    T: NatsClient<M>,
    M: NatsMessageStream,
{
    pub fn new(inner: T, runtime: Arc<Runtime>) -> Self {
        Self {
            inner,
            runtime,
            phantom: PhantomData,
        }
    }

    pub fn publish(
        &self,
        subject: String,
        payload: Bytes,
        headers: Option<HeaderMap>,
    ) -> Result<()> {
        self.runtime
            .block_on(async { self.inner.publish(false, subject, payload, headers).await })
    }

    pub fn create_consumer(
        &self,
        stream_name: StreamName,
        filter_subjects: Vec<Subject>,
        _deliver_policy: DeliverPolicy,
    ) -> Result<BlockingConsumer<M>> {
        self.runtime.block_on(async {
            let (messages, _) = self
                .inner
                .create_consumer_stream(stream_name, filter_subjects, _deliver_policy)
                .await?;
            Ok(BlockingConsumer::new(
                Consumer::new(messages),
                self.runtime.clone(),
            ))
        })
    }
}

pub struct BlockingConsumer<M: NatsMessageStream> {
    consumer: Consumer<M>,
    runtime: Arc<Runtime>,
}

impl<M: NatsMessageStream> BlockingConsumer<M> {
    pub fn new(consumer: Consumer<M>, runtime: Arc<Runtime>) -> BlockingConsumer<M> {
        BlockingConsumer { consumer, runtime }
    }

    pub fn recv_count(
        &mut self,
        timeout: std::time::Duration,
        count: usize,
    ) -> Result<Vec<Message>> {
        self.runtime
            .block_on(async { self.consumer.recv_timeout(timeout, count).await })
    }
}
