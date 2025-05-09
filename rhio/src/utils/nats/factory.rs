use crate::nats::ConsumerId;
use crate::nats::client::types::NatsClient;
use crate::nats::client::types::NatsMessageStream;
use crate::nats::client::types::NatsStreamProtocol;
use crate::utils::retry::types::SeqNo;
use crate::utils::retry::types::StreamFactory;
use anyhow::Context as AnyhowContext;
use async_nats::jetstream::consumer::DeliverPolicy;
use futures::Stream;
use std::future::Future;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use tracing::Span;
use tracing::info;

#[derive(Clone)]
pub struct NatsStreamFactory<N, M: Stream> {
    inner: Arc<NatsStreamFactoryInner<N, M>>,
}

impl<N, M> NatsStreamFactory<N, M>
where
    N: NatsClient<M> + Send + Sync + 'static,
    M: NatsMessageStream + Send + Sync + 'static,
{
    pub fn new(
        client: Arc<N>,
        consumer_id: ConsumerId,
        stream_name: String,
        filter_subjects: Vec<rhio_core::Subject>,
        deliver_policy: DeliverPolicy,
        span: Span,
    ) -> NatsStreamFactory<N, M> {
        NatsStreamFactory {
            inner: Arc::new(NatsStreamFactoryInner {
                client,
                consumer_id,
                stream_name,
                filter_subjects,
                deliver_policy,
                span,
                phantom: PhantomData,
            }),
        }
    }
}

pub struct NatsStreamFactoryInner<N, M> {
    client: Arc<N>,
    consumer_id: ConsumerId,
    stream_name: String,
    filter_subjects: Vec<rhio_core::Subject>,
    deliver_policy: DeliverPolicy,
    span: Span,
    phantom: PhantomData<M>,
}

///
/// Implementation of the `StreamFactory` trait for `NatsStreamFactory`.
///
/// This implementation provides a mechanism to create a stream of NATS messages
/// with optional support for starting from a specific sequence number.
///
/// # Type Parameters
/// - `N`: A type that implements the `NatsClient` trait, responsible for handling
///   NATS client operations.
/// - `M`: A type that implements the `NatsMessageStream` trait, representing the
///   stream of NATS messages.
///
/// # Associated Types
/// - `T`: The protocol type, which is `NatsStreamProtocol`.
/// - `ErrorT`: The error type, which is `anyhow::Error`.
/// - `StreamT`: The stream type, which is `M`.
/// - `Fut`: The future type, which is a pinned boxed future resolving to a result
///   containing the stream (`M`) or an error (`anyhow::Error`).
///
/// # Methods
/// - `create(&self, seq_no: Option<SeqNo>) -> Self::Fut`
///
/// This method creates a new NATS message stream. It accepts an optional sequence
/// number (`seq_no`) to specify the starting point of the stream. If a sequence
/// number is provided, the delivery policy is set to start from that sequence.
/// Otherwise, the default delivery policy is used.
///
/// The method returns a pinned boxed future that resolves to either the created
/// message stream (`M`) or an error (`anyhow::Error`).
///
/// # Parameters
/// - `seq_no`: An optional sequence number (`SeqNo`) indicating the starting point
///   of the message stream.
///
/// # Returns
/// - `Self::Fut`: A pinned boxed future resolving to a result containing the message
///   stream (`M`) or an error (`anyhow::Error`).
///
/// # Errors
/// - Returns an error if the consumer stream cannot be created.
/// - Errors may occur due to issues with the NATS client, invalid parameters, or
///   other runtime conditions.
///
impl<N, M> StreamFactory for NatsStreamFactory<N, M>
where
    N: NatsClient<M> + Send + Sync + 'static,
    M: NatsMessageStream + Send + Sync + 'static,
{
    type T = NatsStreamProtocol;
    type ErrorT = anyhow::Error;
    type StreamT = M;
    type Fut = Pin<Box<dyn Future<Output = Result<M, anyhow::Error>> + Send>>;

    fn create(&self, seq_no: Option<SeqNo>) -> Self::Fut {
        let this = Arc::new(self.inner.clone());
        Box::pin(async move {
            let delivery_policy = seq_no
                .map(|s| DeliverPolicy::ByStartSequence { start_sequence: s })
                .unwrap_or(this.deliver_policy);

            let (messages, consumer_info) = this
                .client
                .create_consumer_stream(
                    this.consumer_id.to_string(),
                    this.stream_name.clone(),
                    this.filter_subjects.clone(),
                    delivery_policy,
                )
                .await
                .context("Cannot create consumer stream")?;

            if consumer_info.num_pending > 0 {
                info!(parent: &this.span, "pending message count {}", consumer_info.num_pending);
            }
            Ok::<M, anyhow::Error>(messages)
        })
    }
}
