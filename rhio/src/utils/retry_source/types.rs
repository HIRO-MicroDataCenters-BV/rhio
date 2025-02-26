use futures::Future;
use std::time::Duration;
pub type StreamFuture<StreamT, FactoryE> =
    dyn Future<Output = Result<Box<StreamT>, FactoryE>> + Send + 'static;
use std::pin::Pin;

/// Stream Item sequence number
pub type SeqNo = u64;

///
/// This trait defines a method for creating a stream, optionally starting from a given sequence number.
///
/// # Type Parameters
///
/// * `StreamT` - The type of the stream to be created.
/// * `FactoryE` - The type of the error that can occur during stream creation.
///
/// # Methods
///
/// * `create` - Creates a new stream, optionally starting from the given sequence number.
///
pub trait StreamFactory<StreamT, FactoryE>: Send + Sync {
    fn create(&self, seq_no: Option<SeqNo>) -> Pin<Box<StreamFuture<StreamT, FactoryE>>>;
}

///
/// This enum represents the different strategies that can be used when a stream error occurs.
/// It provides two variants:
///
/// * `Forward(T)` - This variant indicates that the message should be forwarded as-is.
/// * `WaitRetry(Duration, Option<SeqNo>)` - This variant indicates that the system should wait for a specified duration
///   before making another attempt to recreate the stream, optionally starting from a given sequence number.
///
/// # Variants
///
/// * `Forward(T)` - Forward the message.
/// * `WaitRetry(Duration, Option<SeqNo>)` - Wait for the specified duration and retry, optionally starting from the given sequence number.
///
/// # Type Parameters
///
/// * `T` - The type of the message to be forwarded.
///
#[derive(Debug, Eq, PartialEq)]
pub enum RetryPolicy<T> {
    /// Message Forwarding
    Forward(T),
    /// Wait for a given duration and make another attempt then starting with a sequence number.
    WaitRetry(Duration, Option<SeqNo>),
}

/// This trait defines methods for handling stream messages and factory errors.
///
/// # Type Parameters
///
/// * `T` - The type of the protocol message to be handled.
/// * `FactoryError` - The type of the error that can occur during stream creation.
///
/// # Associated Types
///
/// * `Out` - The type of the output produced by the error handler.
///
/// # Methods
///
/// * `on_stream_msg` - Handles a stream message and returns a `RetryPolicy` indicating the action to be taken.
/// * `on_factory_error` - Handles a factory error and returns a `RetryPolicy` indicating the action to be taken.
///
/// # Methods
///
/// * `on_stream_msg` - Handles a stream message and returns a `RetryPolicy` indicating the action to be taken.
/// * `on_factory_error` - Handles a factory error and returns a `RetryPolicy` indicating the action to be taken.
///
pub trait ErrorHandler<T, FactoryError> {
    type Out;

    fn on_stream_msg(&mut self, protocol: T) -> RetryPolicy<Self::Out>;

    fn on_factory_error(&mut self, attempt: usize, error: FactoryError) -> RetryPolicy<Self::Out>;
}
