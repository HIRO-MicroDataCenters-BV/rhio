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
use crate::utils::retry_source::types::RetryPolicy;
use crate::utils::retry_source::types::StreamFactory;
use crate::utils::retry_source::types::StreamFuture;

pin_project! {
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
            #[pin] delay: tokio::time::Sleep
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
            },
        }
    }
}

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
                RetryStateProj::TimerActive { delay } => {
                    ready!(delay.poll(cx));
                    let factory_method = this.stream_factory.create();
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
                                RetryPolicy::WaitRetry(duration) => RetryState::TimerActive {
                                    delay: tokio::time::sleep(duration),
                                },
                            }
                        }
                    }
                }
                RetryStateProj::WaitingForStream { stream } => {
                    let next_result = ready!(stream.poll_next(cx));
                    match next_result {
                        Some(x) => match this.error_handler.on_stream_msg(x) {
                            RetryPolicy::Forward(element) => return Poll::Ready(Some(element)),
                            RetryPolicy::WaitRetry(duration) => RetryState::TimerActive {
                                delay: tokio::time::sleep(duration),
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
            Ok(vec![Ok(3u8), Ok(4u8), Err(TestError::StreamError)]),
            Ok(vec![Ok(5u8), Ok(6u8)]),
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
            Ok(vec![Ok(3u8), Ok(4u8)]),
        ]);

        let error_action = TestErrorHandler::new(2);

        let data_stream = RetriableStream::new(factory, error_action);
        let actual = data_stream.try_collect::<Vec<u8>>().await?;
        assert_eq!(vec![1, 2, 3, 4], actual,);

        Ok(())
    }

    pub struct TestErrorHandler {
        max_attempts: usize,
    }

    impl TestErrorHandler {
        pub fn new(max_attempts: usize) -> Self {
            TestErrorHandler { max_attempts }
        }
    }

    impl ErrorHandler<Result<u8, TestError>, TestError> for TestErrorHandler {
        type Out = Result<u8, TestError>;

        fn on_stream_msg(
            &mut self,
            e: Result<u8, TestError>,
        ) -> RetryPolicy<Result<u8, TestError>> {
            match e {
                Ok(element) => RetryPolicy::Forward(Ok(element)),
                Err(_) => RetryPolicy::WaitRetry(Duration::from_millis(1)),
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
                RetryPolicy::WaitRetry(Duration::from_millis(1))
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

    #[derive(Clone)]
    struct TestStreamFactory {
        items: Vec<Result<Vec<Result<u8, TestError>>, TestError>>,
        counter: Arc<AtomicUsize>,
    }

    impl TestStreamFactory {
        pub fn new(items: Vec<Result<Vec<Result<u8, TestError>>, TestError>>) -> TestStreamFactory {
            TestStreamFactory {
                items,
                counter: Arc::new(AtomicUsize::new(0)),
            }
        }
    }

    impl StreamFactory<MyStream, TestError> for TestStreamFactory {
        fn create(&self) -> Pin<Box<StreamFuture<MyStream, TestError>>> {
            let this = self.clone();
            Box::pin(async move {
                let index = this.counter.fetch_add(1, Ordering::AcqRel);
                let maybe_items: Result<Vec<Result<u8, TestError>>, TestError> = this
                    .items
                    .get(index)
                    .map(|items| items.clone())
                    .unwrap_or(Ok(vec![]));
                let items = maybe_items?;
                let stream: Box<MyStream> = Box::new(MyStream {
                    items: Box::new(futures::stream::iter(items)),
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
