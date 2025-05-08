use futures::stream::Stream;
use futures::{ready, TryFuture};
use pin_project_lite::pin_project;
use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use super::types::ErrorHandler;
use crate::utils::retry::types::{RetryPolicy, SeqNo, StreamFactory};

pin_project! {
    /// `RetriableStream` is a wrapper around a stream that provides retry logic for handling errors
    /// during streaming. It uses a factory to create new instances of the stream when retries are
    /// required, and an error handler to determine the retry policy.
    ///
    /// # Type Parameters
    ///
    /// * `Factory` - The type of the factory used to create new stream instances.
    /// * `F` - The type of the error handler used to determine the retry policy.
    ///
    /// # Fields
    ///
    /// * `state` - Represents the current state of the retry logic, which can be:
    ///   - Waiting for a stream to produce items.
    ///   - Waiting for the factory to create a new stream.
    ///   - Waiting for a retry timer to elapse.
    /// * `error_handler` - The error handler that determines the retry policy based on errors
    ///   encountered during streaming or factory creation.
    /// * `stream_factory` - The factory used to create new stream instances.
    /// * `attempt` - Tracks the current retry attempt count.
    ///
    /// # Behavior
    ///
    /// The `RetriableStream` transitions between states based on the outcome of the underlying
    /// stream or factory:
    ///
    /// - If the stream produces an error, the error handler determines whether to retry or forward
    ///   the error.
    /// - If the factory fails to create a stream, the error handler determines whether to retry or
    ///   forward the error.
    /// - If a retry is required, a timer is started before attempting to create a new stream.
    ///
    pub struct RetriableStream<Factory, F>
    where
        Factory: StreamFactory,
    {
        #[pin]
        state: RetryState<Factory>,
        error_handler: F,
        stream_factory: Arc<Factory>,
        attempt: usize
    }
}

pin_project! {
    #[project = RetryStateProj]
    enum RetryState<Factory: StreamFactory>
    {

        WaitingForStream {
            #[pin]
            stream: Factory::StreamT,
        },

        WaitingForStreamFactory {
            #[pin]
            factory_method: Factory::Fut,
        },

        TimerActive {
            #[pin] delay: tokio::time::Sleep, seq_no: Option<SeqNo>
        },
    }
}

///
/// This implementation provides the core logic for consuming items from the wrapped stream
/// while handling errors and applying retry policies. The `RetriableStream` transitions
/// between different states (`TimerActive`, `WaitingForStreamFactory`, and `WaitingForStream`)
/// to manage retries and recover from errors.
///
/// # Type Parameters
///
/// * `Factory` - The type of the factory used to create new stream instances.
/// * `F` - The type of the error handler used to determine the retry policy.
///
/// # Behavior
///
/// - The `poll_next` method is responsible for driving the state machine of the `RetriableStream`.
/// - When an error occurs in the stream or during stream creation, the error handler determines
///   whether to retry or forward the error.
/// - If a retry is required, a delay is introduced before attempting to create a new stream.
/// - The retry attempt count is reset when a new stream is successfully created.
///
/// # States
///
/// - `TimerActive`: A delay is active before retrying the stream creation.
/// - `WaitingForStreamFactory`: Waiting for the factory to create a new stream instance.
/// - `WaitingForStream`: Consuming items from the active stream.
///
/// # Methods
///
/// - `poll_next`: Polls the next item from the stream, handling retries and errors as needed.
///
impl<Factory, F> RetriableStream<Factory, F>
where
    Factory: StreamFactory,
    F: ErrorHandler<Factory::T, Factory::ErrorT, Out = Factory::T>,
{
    pub fn new(stream_factory: Factory, error_handler: F) -> Self {
        Self {
            error_handler,
            stream_factory: Arc::new(stream_factory),
            attempt: 1,
            state: RetryState::TimerActive {
                delay: tokio::time::sleep(Duration::from_millis(1)),
                seq_no: None,
            },
        }
    }
}

impl<Factory: StreamFactory, F> Stream for RetriableStream<Factory, F>
where
    F: ErrorHandler<Factory::T, <Factory::Fut as TryFuture>::Error, Out = Factory::T>,
{
    type Item = Factory::T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        loop {
            let this = self.as_mut().project();
            let attempt = *this.attempt;
            let new_state = match this.state.project() {
                RetryStateProj::TimerActive { delay, seq_no } => {
                    ready!(delay.poll(cx));
                    let factory_method = this.stream_factory.create(*seq_no);
                    RetryState::WaitingForStreamFactory { factory_method }
                }
                RetryStateProj::WaitingForStreamFactory { factory_method, .. } => {
                    let maybe_result = ready!(factory_method.poll(cx));
                    match maybe_result {
                        Ok(stream) => {
                            *this.attempt = 1;
                            RetryState::WaitingForStream { stream }
                        }
                        Err(e) => {
                            *this.attempt += 1;
                            match this.error_handler.on_factory_error(attempt, e) {
                                RetryPolicy::Forward(element) => return Poll::Ready(Some(element)),
                                RetryPolicy::WaitRetry(duration, seq_no) => {
                                    RetryState::TimerActive {
                                        delay: tokio::time::sleep(duration),
                                        seq_no,
                                    }
                                }
                            }
                        }
                    }
                }
                RetryStateProj::WaitingForStream { stream } => {
                    let next_result = ready!(stream.poll_next(cx));
                    match next_result {
                        Some(x) => match this.error_handler.on_stream_msg(x) {
                            RetryPolicy::Forward(element) => return Poll::Ready(Some(element)),
                            RetryPolicy::WaitRetry(duration, seq_no) => RetryState::TimerActive {
                                delay: tokio::time::sleep(duration),
                                seq_no,
                            },
                        },
                        None => return Poll::Ready(None),
                    }
                }
            };
            self.as_mut().project().state.set(new_state);
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, None)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use anyhow::Result;
    use futures_util::TryStreamExt;
    use std::sync::atomic::Ordering;
    use std::sync::{atomic::AtomicUsize, Arc};
    use thiserror::Error;

    #[tokio::test]
    async fn test_empty() -> Result<()> {
        let factory = TestStreamFactory::new(vec![]);

        let error_action = TestErrorHandler::new(3);

        let data_stream = RetriableStream::new(factory, error_action);
        let actual = data_stream.try_collect::<Vec<u8>>().await?;
        assert_eq!(Vec::<u8>::new(), actual);

        Ok(())
    }

    #[tokio::test]
    async fn test_factory_error_empty_result() -> Result<()> {
        let factory = TestStreamFactory::new(vec![Err(TestError::FactoryError), Ok(vec![])]);

        let error_action = TestErrorHandler::new(3);

        let data_stream = RetriableStream::new(factory, error_action);
        let actual = data_stream.try_collect::<Vec<u8>>().await?;
        assert_eq!(Vec::<u8>::new(), actual);

        Ok(())
    }

    #[tokio::test]
    async fn test_factory_error_retries_exhausted() -> Result<()> {
        let factory = TestStreamFactory::new(vec![
            Err(TestError::FactoryError),
            Err(TestError::FactoryError),
        ]);

        let error_action = TestErrorHandler::new(1);
        let data_stream = RetriableStream::new(factory, error_action);
        assert!(data_stream.try_collect::<Vec<u8>>().await.is_err());

        Ok(())
    }

    #[tokio::test]
    async fn test_reset_attempt_count() -> Result<()> {
        let factory = TestStreamFactory::new(vec![
            Err(TestError::FactoryError),
            Err(TestError::FactoryError),
            Ok(vec![Ok(1u8), Err(TestError::StreamError)]),
            Err(TestError::FactoryError),
            Err(TestError::FactoryError),
            Ok(vec![Ok(1u8)]),
        ]);

        let error_action = TestErrorHandler::new(3);
        let data_stream = RetriableStream::new(factory, error_action);
        let actual = data_stream.try_collect::<Vec<u8>>().await?;
        assert_eq!(vec![1], actual);

        Ok(())
    }

    #[tokio::test]
    async fn test_no_failure() -> Result<()> {
        let factory = TestStreamFactory::new(vec![Ok(vec![Ok(1u8), Ok(2u8)])]);

        let error_action = TestErrorHandler::new(3);

        let data_stream = RetriableStream::new(factory, error_action);
        let actual = data_stream.try_collect::<Vec<u8>>().await?;
        assert_eq!(vec![1, 2], actual);

        Ok(())
    }

    #[tokio::test]
    async fn test_basic_retry() -> Result<()> {
        let factory = TestStreamFactory::new(vec![
            Ok(vec![Ok(1u8), Ok(2u8), Err(TestError::StreamError)]),
            Ok(vec![
                Ok(1u8),
                Ok(2u8),
                Ok(3u8),
                Ok(4u8),
                Err(TestError::StreamError),
            ]),
            Ok(vec![Ok(1u8), Ok(2u8), Ok(3u8), Ok(4u8), Ok(5u8), Ok(6u8)]),
        ]);

        let error_action = TestErrorHandler::new(2);

        let data_stream = RetriableStream::new(factory, error_action);
        let actual = data_stream.try_collect::<Vec<u8>>().await?;
        assert_eq!(vec![1, 2, 3, 4, 5, 6], actual,);

        Ok(())
    }

    #[tokio::test]
    async fn test_basic_retry_with_factory_failures() -> Result<()> {
        let factory = TestStreamFactory::new(vec![
            Ok(vec![Ok(1u8), Ok(2u8), Err(TestError::StreamError)]),
            Err(TestError::StreamError),
            Ok(vec![Ok(1u8), Ok(2u8), Ok(3u8), Ok(4u8)]),
        ]);

        let error_action = TestErrorHandler::new(2);

        let data_stream = RetriableStream::new(factory, error_action);
        let actual = data_stream.try_collect::<Vec<u8>>().await?;
        assert_eq!(vec![1, 2, 3, 4], actual,);

        Ok(())
    }

    pub struct TestErrorHandler {
        max_attempts: usize,
        seq_no: Option<SeqNo>,
    }

    impl TestErrorHandler {
        pub fn new(max_attempts: usize) -> Self {
            TestErrorHandler {
                max_attempts,
                seq_no: None,
            }
        }
    }

    impl ErrorHandler<Result<u8, TestError>, TestError> for TestErrorHandler {
        type Out = Result<u8, TestError>;

        fn on_stream_msg(
            &mut self,
            e: Result<u8, TestError>,
        ) -> RetryPolicy<Result<u8, TestError>> {
            match e {
                Ok(element) => {
                    self.seq_no = Some(element as SeqNo);
                    RetryPolicy::Forward(Ok(element))
                }
                Err(_) => RetryPolicy::WaitRetry(Duration::from_millis(1), self.seq_no.clone()),
            }
        }

        fn on_factory_error(
            &mut self,
            current_attempt: usize,
            e: TestError,
        ) -> RetryPolicy<Result<u8, TestError>> {
            if current_attempt >= self.max_attempts {
                return RetryPolicy::Forward(Err(e));
            } else {
                RetryPolicy::WaitRetry(Duration::from_millis(1), self.seq_no.clone())
            }
        }
    }

    pin_project! {
        struct MyStream {
            #[pin]
            items: Box<dyn Stream<Item = Result<u8, TestError>> + Unpin>
        }
    }

    #[allow(dead_code)]
    trait RetryableStream: Stream<Item = Self::StreamItem> + Sized {
        type StreamItem;
    }

    impl RetryableStream for MyStream {
        type StreamItem = Result<u8, TestError>;
    }

    impl Stream for MyStream {
        type Item = Result<u8, TestError>;

        fn poll_next(
            self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Option<Self::Item>> {
            let this = self.project();
            this.items.poll_next(cx)
        }
    }

    type StreamItem = Result<u8, TestError>;
    type StreamItems = Vec<StreamItem>;
    type ConnectionAttempt = Result<StreamItems, TestError>;

    #[derive(Clone)]
    struct TestStreamFactory {
        items: Vec<ConnectionAttempt>,
        counter: Arc<AtomicUsize>,
    }

    impl TestStreamFactory {
        pub fn new(items: Vec<ConnectionAttempt>) -> TestStreamFactory {
            TestStreamFactory {
                items,
                counter: Arc::new(AtomicUsize::new(0)),
            }
        }
    }

    impl StreamFactory for TestStreamFactory {
        type T = Result<u8, TestError>;
        type ErrorT = TestError;
        type StreamT = MyStream;
        type Fut = Pin<Box<dyn Future<Output = Result<MyStream, TestError>>>>;

        fn create(&self, seq_no: Option<SeqNo>) -> Self::Fut {
            let this = self.clone();
            Box::pin(async move {
                let connection_attempt_no = this.counter.fetch_add(1, Ordering::AcqRel);
                let items_for_connection: ConnectionAttempt = this
                    .items
                    .get(connection_attempt_no)
                    .map(|items| items.clone())
                    .unwrap_or(Ok(vec![]));
                let items = items_for_connection?;
                let start_idx = seq_no.unwrap_or(0) as usize;
                let to_return = items[start_idx..].to_vec();
                let stream: MyStream = MyStream {
                    items: Box::new(futures::stream::iter(to_return)),
                };
                Ok::<MyStream, TestError>(stream)
            })
        }
    }

    #[derive(Error, Debug, Clone)]
    pub enum TestError {
        #[error("factory error")]
        FactoryError,
        #[error("stream error")]
        StreamError,
    }
}
