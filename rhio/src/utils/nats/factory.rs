use crate::nats::client::types::NatsClient;
use crate::nats::client::types::NatsMessageStream;
use crate::nats::ConsumerId;
use crate::utils::retry::types::SeqNo;
use crate::utils::retry::types::StreamFactory;
use crate::utils::retry::types::StreamFuture;
use anyhow::Context as AnyhowContext;
use async_nats::jetstream::consumer::DeliverPolicy;
use futures::Stream;
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use tracing::info;
use tracing::Span;

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
/// This implementation allows the creation of a stream of NATS messages with optional sequence number support.
///
/// # Type Parameters
/// - `N`: A type that implements the `NatsClient` trait for handling NATS client operations.
/// - `M`: A type that implements the `NatsMessageStream` trait for handling NATS message streams.
///
/// # Methods
/// - `create(&self, seq_no: Option<SeqNo>) -> Pin<Box<StreamFuture<M, anyhow::Error>>>`
///
/// This method creates a new NATS message stream. It takes an optional sequence number (`seq_no`) which, if provided,
/// sets the delivery policy to start from the specified sequence number. If no sequence number is provided, the default
/// delivery policy is used.
///
/// The method returns a pinned future that resolves to a boxed stream of NATS messages or an error.
///
/// # Parameters
/// - `seq_no`: An optional sequence number (`SeqNo`) to start the message stream from.
///
/// # Returns
/// - `Pin<Box<StreamFuture<M, anyhow::Error>>>`: A pinned future that resolves to a boxed stream of NATS messages or an error.
///
/// # Errors
/// - Returns an error if the consumer stream cannot be created.
///
impl<N, M> StreamFactory<M, anyhow::Error> for NatsStreamFactory<N, M>
where
    N: NatsClient<M> + Send + Sync + 'static,
    M: NatsMessageStream + Send + Sync + 'static,
{
    fn create(&self, seq_no: Option<SeqNo>) -> Pin<Box<StreamFuture<M, anyhow::Error>>> {
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
            Ok::<Box<M>, anyhow::Error>(Box::new(messages))
        })
    }
}
