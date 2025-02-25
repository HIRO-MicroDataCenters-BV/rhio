use futures::Future;
use std::time::Duration;
pub type StreamFuture<StreamT, FactoryE> =
    dyn Future<Output = Result<Box<StreamT>, FactoryE>> + Send + 'static;
use std::pin::Pin;
pub trait StreamFactory<StreamT, FactoryE>: Send + Sync {
    fn create(&self) -> Pin<Box<StreamFuture<StreamT, FactoryE>>>;
}

#[derive(Debug, Eq, PartialEq)]
pub enum RetryPolicy<T> {
    /// Message Forwarding
    Forward(T),
    /// Wait for a given duration and make another attempt then.
    WaitRetry(Duration),
}

pub trait ErrorHandler<T, FactoryError> {
    type Out;

    fn on_stream_msg(&mut self, protocol: T) -> RetryPolicy<Self::Out>;

    fn on_factory_error(&mut self, attempt: usize, error: FactoryError) -> RetryPolicy<Self::Out>;
}
