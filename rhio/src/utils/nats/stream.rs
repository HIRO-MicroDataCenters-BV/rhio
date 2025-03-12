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

pub type RecoverableNatsStream = dyn Stream<Item = NatsStreamProtocol> + Send + 'static;

/// the `NatsMessageStream` and `Stream` traits. It provides a mechanism to create a stream
/// that can recover from errors using a retry strategy.
///
/// # Fields
/// - `stream`: A pinned box containing the recoverable NATS stream.
///
/// # Methods
/// - `new`: Creates a new instance of `RecoverableNatsStreamImpl` with the given factory,
///   retry options, and tracing span.
///
#[pin_project]
pub struct RecoverableNatsStreamImpl {
    #[pin]
    stream: Pin<Box<RecoverableNatsStream>>,
}

impl RecoverableNatsStreamImpl {
    pub fn new<N, M>(
        factory: NatsStreamFactory<N, M>,
        retry_options: RetryConfig,
        span: Span,
    ) -> RecoverableNatsStreamImpl
    where
        M: NatsMessageStream + Send + Sync + Unpin + 'static,
        N: NatsClient<M> + Send + Sync + 'static,
    {
        let error_handler = NatsErrorHandler::new(retry_options, span);
        let stream = Box::pin(RetriableStream::new(factory, error_handler));

        RecoverableNatsStreamImpl { stream }
    }
}

impl NatsMessageStream for RecoverableNatsStreamImpl {}

impl Stream for RecoverableNatsStreamImpl {
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
            client::fake::{client::FakeNatsClient, server::FakeNatsServer},
            ConsumerId,
        },
        tests::configuration::generate_nats_config,
        tracing::setup_tracing,
    };
    use anyhow::Context;
    use anyhow::{anyhow, Result};
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
        pub fn new(mut stream: RecoverableNatsStreamImpl) -> ActiveConsumer {
            let results = Arc::new(Mutex::new(vec![]));
            let local_result = results.clone();
            tokio::spawn(async move {
                loop {
                    if let Some(value) = stream.next().await {
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
