use futures::Stream;
use futures::TryFuture;
use std::time::Duration;

/// Stream Item sequence number
pub type SeqNo = u64;

/// A trait for creating streams with optional sequence number starting points.
///
/// The `StreamFactory` trait provides an abstraction for creating streams that can start
/// from a specific sequence number. It is designed to be generic and flexible, allowing
/// implementers to define the types of streams, items, and errors they work with.
///
/// # Associated Types
///
/// * `T` - The type of the items produced by the stream.
/// * `ErrorT` - The type of the error that can occur during stream creation.
/// * `StreamT` - The type of the stream to be created, which must implement the `Stream` trait
///   and produce items of type `T`.
/// * `Fut` - The type of the future returned by the `create` method, which must implement the
///   `TryFuture` trait and resolve to a `Result` containing the created stream or an error.
///
/// # Required Methods
///
/// * `create` - Creates a new stream, optionally starting from the given sequence number.
///   This method returns a future that resolves to the created stream or an error.
///
pub trait StreamFactory {
    type T;
    type ErrorT;
    type StreamT: Stream<Item = Self::T>;
    type Fut: TryFuture<
            Ok = Self::StreamT,
            Error = Self::ErrorT,
            Output = Result<Self::StreamT, Self::ErrorT>,
        >;

    fn create(&self, seq_no: Option<SeqNo>) -> Self::Fut;
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
