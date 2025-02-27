use futures::ready;
use futures::stream::Stream;
use pin_project_lite::pin_project;
use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use super::types::ErrorHandler;
use crate::utils::retry_source::types::StreamFactory;
use crate::utils::retry_source::types::StreamFuture;
use crate::utils::retry_source::types::{RetryPolicy, SeqNo};

pin_project! {
    ///
    /// `RetriableStream` wraps around another stream and provides retry logic
    /// for handling errors that occur during streaming. It uses a factory to
    /// create new instances of the stream when retries are needed.
    ///
    /// # Type Parameters
    ///
    /// * `StreamT` - The type of the underlying stream.
    /// * `Factory` - The type of the factory used to create new stream instances.
    /// * `FactoryE` - The type of error that the factory can produce.
    /// * `F` - The type of the error handler used to determine the retry policy.
    ///
    /// # Fields
    ///
    /// * `state` - The current state of the retry logic, which can be waiting for a stream,
    ///   waiting for the factory to create a stream, or waiting for a retry timer.
    /// * `error_handler` - The error handler that determines the retry policy based on errors.
    /// * `stream_factory` - The factory used to create new stream instances.
    /// * `attempt` - The current retry attempt count.
    ///
    pub struct RetriableStream<StreamT, Factory: StreamFactory<StreamT, FactoryE>, FactoryE, F>{
        #[pin]
        state: RetryState<StreamT, FactoryE>,
        error_handler: F,
        stream_factory: Arc<Factory>,
        attempt: usize
    }
}

pin_project! {
    #[project = RetryStateProj]
    enum RetryState<StreamT, FactoryE>
    {
        WaitingForStream{ #[pin] stream: Box<StreamT> },
        WaitingForStreamFactory {
            #[pin]
            factory_method: Pin<Box<StreamFuture<StreamT, FactoryE>>>,
        },
        TimerActive {
            #[pin] delay: tokio::time::Sleep, seq_no: Option<SeqNo>
        },
    }
}

impl<StreamT, T, Factory, FactoryE, F> RetriableStream<StreamT, Factory, FactoryE, F>
where
    F: ErrorHandler<T, FactoryE, Out = T>,
    StreamT: Stream<Item = T>,
    Factory: StreamFactory<StreamT, FactoryE>,
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
///
/// This implementation provides the core logic for polling the underlying stream
/// and handling retries when errors occur. The `poll_next` method is responsible
/// for driving the state machine that manages the retry logic.
///
/// # Type Parameters
///
/// * `StreamT` - The type of the underlying stream.
/// * `T` - The type of items produced by the stream.
/// * `Factory` - The type of the factory used to create new stream instances.
/// * `FactoryE` - The type of error that the factory can produce.
/// * `F` - The type of the error handler used to determine the retry policy.
///
/// # Polling Logic
///
/// The `poll_next` method uses a loop to continuously poll the current state of the
/// retry logic until a stream item is produced or the stream is exhausted. The state
/// transitions are as follows:
///
/// * `TimerActive` - Waits for a retry timer to complete before attempting to create
///   a new stream instance using the factory.
/// * `WaitingForStreamFactory` - Waits for the factory to create a new stream instance.
///   If the factory produces an error, the error handler is consulted to determine the
///   retry policy.
/// * `WaitingForStream` - Polls the underlying stream for the next item. If an error
///   occurs, the error handler is consulted to determine the retry policy.
///
impl<StreamT, T, Factory, FactoryE, F> Stream for RetriableStream<StreamT, Factory, FactoryE, F>
where
    F: ErrorHandler<T, FactoryE, Out = T>,
    StreamT: Stream<Item = T> + Unpin + 'static,
    Factory: StreamFactory<StreamT, FactoryE>,
{
    type Item = StreamT::Item;

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
                        Ok(stream) => RetryState::WaitingForStream { stream },
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

    impl StreamFactory<MyStream, TestError> for TestStreamFactory {
        fn create(&self, seq_no: Option<SeqNo>) -> Pin<Box<StreamFuture<MyStream, TestError>>> {
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
                let stream: Box<MyStream> = Box::new(MyStream {
                    items: Box::new(futures::stream::iter(to_return)),
                });
                Ok::<Box<MyStream>, TestError>(stream)
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
