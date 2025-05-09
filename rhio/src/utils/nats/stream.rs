use crate::nats::client::types::NatsClient;
use crate::nats::client::types::NatsMessageStream;
use crate::nats::client::types::NatsStreamProtocol;
use crate::utils::retry::stream::RetriableStream;

use futures::Stream;
use pin_project::pin_project;
use std::pin::Pin;
use std::task::{Context, Poll};
use tracing::Span;

use super::error::NatsErrorHandler;
use super::error::RetryConfig;
use super::factory::NatsStreamFactory;

/// A recoverable NATS message stream implementation.
///
/// `RecoverableNatsStreamImpl` is a wrapper around a NATS message stream that provides
/// error recovery and retry capabilities. It is designed to handle transient errors
/// in the underlying NATS stream by using a retry strategy, ensuring that the stream
/// remains operational even in the presence of temporary failures.
///
/// This implementation leverages the `RetriableStream` to manage retries and error
/// handling, and it integrates with tracing spans for better observability.
///
/// # Type Parameters
/// - `N`: The type of the NATS client that implements the `NatsClient` trait.
/// - `M`: The type of the NATS message stream that implements the `NatsMessageStream` trait.
///
/// # Fields
/// - `stream`: A pinned `RetriableStream` that wraps the NATS stream factory and error handler.
///
/// # Methods
/// - `new`: Constructs a new instance of `RecoverableNatsStreamImpl` with the provided
///   stream factory, retry configuration, and tracing span.
///
/// # Traits Implemented
/// - `Stream`: Allows the `RecoverableNatsStreamImpl` to be used as an asynchronous stream,
///   yielding items of type `NatsStreamProtocol`.
/// - `NatsMessageStream`: Implements the NATS-specific message stream trait.
///
#[pin_project]
pub struct RecoverableNatsStreamImpl<N, M>
where
    M: NatsMessageStream + Send + Sync + 'static,
    N: NatsClient<M> + Send + Sync + 'static,
{
    #[pin]
    stream: RetriableStream<NatsStreamFactory<N, M>, NatsErrorHandler>,
}

impl<N, M> RecoverableNatsStreamImpl<N, M>
where
    M: NatsMessageStream + Send + Sync + 'static,
    N: NatsClient<M> + Send + Sync + 'static,
{
    pub fn new(
        factory: NatsStreamFactory<N, M>,
        retry_options: RetryConfig,
        span: Span,
    ) -> RecoverableNatsStreamImpl<N, M> {
        let error_handler = NatsErrorHandler::new(retry_options, span);
        let stream: RetriableStream<NatsStreamFactory<N, M>, NatsErrorHandler> =
            RetriableStream::new(factory, error_handler);

        RecoverableNatsStreamImpl { stream }
    }
}

impl<N, M> NatsMessageStream for RecoverableNatsStreamImpl<N, M>
where
    M: NatsMessageStream + Send + Sync + 'static,
    N: NatsClient<M> + Send + Sync + 'static,
{
}

impl<N, M> Stream for RecoverableNatsStreamImpl<N, M>
where
    M: NatsMessageStream + Send + Sync + 'static,
    N: NatsClient<M> + Send + Sync + 'static,
{
    type Item = NatsStreamProtocol;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        this.stream.poll_next(cx)
    }
}

#[cfg(test)]
pub mod tests {
    use super::*;

    use crate::{
        nats::{
            ConsumerId,
            client::fake::{client::FakeNatsClient, server::FakeNatsServer},
        },
        tests::configuration::generate_nats_config,
        tracing::setup_tracing,
    };
    use anyhow::Context;
    use anyhow::{Result, anyhow};
    use async_nats::jetstream::consumer::DeliverPolicy;
    use futures::StreamExt;
    use rhio_core::Subject;
    use std::{str::FromStr, sync::Arc, time::Duration};
    use tokio::sync::Mutex;
    use tracing::info;

    #[tokio::test(flavor = "multi_thread", worker_threads = 5)]
    async fn test_stream_reconnect() -> Result<()> {
        setup_tracing(Some("=INFO".into()));

        let RecoverableNatsStreamTestSetup {
            client,
            server,
            consumer,
            subject,
        } = setup_test().await?;

        server
            .wait_for_connections(Duration::from_secs(5))
            .await
            .context("waiting for connection")?;

        client
            .publish_test_messages(&subject, &vec![1, 2, 3])
            .await
            .context("publishing messages [1,2,3]")?;

        let actual_msgs = consumer
            .wait(3, Duration::from_secs(5))
            .await
            .context("receiving messages [1,2,3]")?;
        assert_messages(actual_msgs, &vec![1, 2, 3])?;

        server.enable_connection_error();
        server
            .wait_for_connection_error(Duration::from_secs(10))
            .await
            .context("waiting for connection errors")?;
        server.disable_connection_error();

        client
            .publish_test_messages(&subject, &vec![4, 5, 6])
            .await
            .context("publishing messages [4,5,6]")?;

        let actual_msg_final = consumer.wait(6, Duration::from_millis(5000)).await?;
        assert_messages(actual_msg_final, &vec![1, 2, 3, 4, 5, 6])?;

        Ok(())
    }

    struct RecoverableNatsStreamTestSetup {
        client: Arc<FakeNatsClient>,
        server: Arc<FakeNatsServer>,
        consumer: ActiveConsumer,
        subject: String,
    }

    async fn setup_test() -> Result<RecoverableNatsStreamTestSetup> {
        let nats_config = generate_nats_config();
        let span = Span::none();

        let client =
            Arc::new(FakeNatsClient::new(nats_config.clone()).context("Source FakeNatsClient")?);

        let server =
            FakeNatsServer::get_by_config(&nats_config).context("no fake NATS server exists")?;

        let subject = String::from("filter_subject");
        let jetstream = String::from("jetstream");
        let factory = NatsStreamFactory::new(
            client.clone(),
            ConsumerId::new(jetstream.clone(), subject.clone().into()),
            jetstream,
            vec![Subject::from_str(&subject)?],
            DeliverPolicy::All,
            span.clone(),
        );
        let retry_options = RetryConfig {
            max_attempts: None,
            min_delay: Duration::from_millis(100),
            max_delay: Duration::from_millis(1000),
        };
        let stream = RecoverableNatsStreamImpl::new(factory, retry_options, span.clone());
        let consumer = ActiveConsumer::new(stream);

        Ok(RecoverableNatsStreamTestSetup {
            client,
            server,
            consumer,
            subject,
        })
    }

    fn assert_messages(actual_messages: Vec<NatsStreamProtocol>, ids: &Vec<usize>) -> Result<()> {
        let actual_payloads = actual_messages
            .into_iter()
            .map(|m| match m {
                NatsStreamProtocol::Msg { msg, .. } => {
                    let payload_str = String::from_utf8(msg.payload.to_vec())
                        .context("unable to convert payload to string")?;
                    Ok(payload_str)
                }
                other_msg => Err(anyhow!("Unexpected message {:?}", other_msg)),
            })
            .collect::<Result<Vec<String>>>()?;
        let expected_payloads: Vec<String> =
            ids.into_iter().map(|id| format!("message {id}")).collect();
        assert_eq!(actual_payloads, expected_payloads);
        Ok(())
    }

    struct ActiveConsumer {
        results: Arc<Mutex<Vec<NatsStreamProtocol>>>,
    }

    impl ActiveConsumer {
        pub fn new<N, M>(stream: RecoverableNatsStreamImpl<N, M>) -> ActiveConsumer
        where
            M: NatsMessageStream + Send + Sync + 'static,
            N: NatsClient<M> + Send + Sync + 'static,
        {
            let results = Arc::new(Mutex::new(vec![]));
            let local_result = results.clone();
            tokio::spawn(async move {
                let mut s = Box::pin(stream);
                loop {
                    if let Some(value) = s.next().await {
                        let mut result_guard = local_result.lock().await;
                        result_guard.push(value);
                        drop(result_guard);
                    } else {
                        break;
                    }
                }
                info!("active consumer ended");
            });
            ActiveConsumer { results }
        }

        async fn wait(&self, n: usize, duration: Duration) -> Result<Vec<NatsStreamProtocol>> {
            tokio::time::timeout(duration, async {
                loop {
                    if let Ok(results) = self.results.try_lock() {
                        if results.len() >= n {
                            return Ok(results.clone());
                        }
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await?
        }
    }
}
