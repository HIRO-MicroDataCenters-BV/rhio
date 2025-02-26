use crate::nats::client::types::NatsStreamProtocol;
use crate::utils::retry_source::types::ErrorHandler;
use crate::utils::retry_source::types::RetryPolicy;
use crate::utils::retry_source::types::SeqNo;
use serde::Deserialize;
use serde::Serialize;
use tokio::time::Duration;
use tracing::Span;
use tracing::{error, info, warn};

/// Configuration for retrying operations in the NATS error handler.
///
/// # Fields
///
/// * `max_attempts` - The maximum number of retry attempts. If `None`, there is no limit on the number of attempts.
/// * `min_delay` - The minimum delay between retry attempts.
/// * `max_delay` - The maximum delay between retry attempts.
///
/// # Default Values
///
/// The default values for `RetryConfig` are:
/// * `max_attempts`: `None` (no limit on retry attempts)
/// * `min_delay`: 1000 milliseconds
/// * `max_delay`: 10000 milliseconds
///
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RetryConfig {
    pub max_attempts: Option<usize>,
    pub min_delay: Duration,
    pub max_delay: Duration,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: None,
            min_delay: Duration::from_millis(1000),
            max_delay: Duration::from_millis(10000),
        }
    }
}

pub struct NatsErrorHandler {
    options: RetryConfig,
    seq_no: Option<SeqNo>,
    span: Span,
}

impl NatsErrorHandler {
    pub fn new(options: RetryConfig, span: Span) -> Self {
        NatsErrorHandler {
            options,
            span,
            seq_no: None,
        }
    }
}
/// Implementation of the `ErrorHandler` trait for `NatsErrorHandler`.
///
/// This implementation defines how the `NatsErrorHandler` handles incoming stream messages and factory errors
/// for the NATS streaming protocol.
///
/// # Associated Types
///
/// * `Out` - The output type, which is `NatsStreamProtocol`.
///
/// # Methods
///
/// ## `on_stream_msg`
///
/// Handles incoming stream messages and determines the retry policy based on the message type.
///
/// * `incoming` - The incoming `NatsStreamProtocol` message.
///
/// Returns a `RetryPolicy<NatsStreamProtocol>` indicating whether to forward the message or wait and retry.
///
/// - If the message is of type `Msg`, it updates the sequence number and forwards the message.
/// - If the message is of type `ServerDisconnect`, it waits and retries after the minimum delay.
/// - If the message is of type `Error`, it logs the error and waits and retries after the minimum delay.
///
/// ## `on_factory_error`
///
/// Handles errors that occur during the creation of the stream and determines the retry policy based on the current attempt number and the error.
///
/// * `current_attempt` - The current attempt number.
/// * `e` - The error that occurred.
///
/// Returns a `RetryPolicy<NatsStreamProtocol>` indicating whether to forward an error message or wait and retry.
///
/// - If the current attempt number exceeds the maximum number of attempts, it logs a warning and forwards an error message indicating that it is giving up.
/// - Otherwise, it calculates a delay based on the current attempt number and the configured minimum and maximum delays, logs the error, and waits and retries after the calculated delay.
///
impl ErrorHandler<NatsStreamProtocol, anyhow::Error> for NatsErrorHandler {
    type Out = NatsStreamProtocol;

    fn on_stream_msg(&mut self, incoming: NatsStreamProtocol) -> RetryPolicy<NatsStreamProtocol> {
        match &incoming {
            NatsStreamProtocol::Msg { seq, .. } => {
                self.seq_no = seq.map(|s| s + 1).or(self.seq_no);
                RetryPolicy::Forward(incoming)
            }
            NatsStreamProtocol::ServerDisconnect => {
                RetryPolicy::WaitRetry(self.options.min_delay, self.seq_no)
            }
            NatsStreamProtocol::Error { msg } => {
                error!(parent: &self.span, "Stream error {msg}, attempting to recover...");
                RetryPolicy::WaitRetry(self.options.min_delay, self.seq_no)
            }
        }
    }

    fn on_factory_error(
        &mut self,
        current_attempt: usize,
        e: anyhow::Error,
    ) -> RetryPolicy<NatsStreamProtocol> {
        if current_attempt >= self.options.max_attempts.unwrap_or(usize::MAX) {
            warn!(parent: &self.span, "Stream factory failure({e}): no retry, current_attempt={current_attempt}");
            return RetryPolicy::Forward(NatsStreamProtocol::Error {
                msg: format!("Giving up reconnecting after {current_attempt} attempts"),
            });
        }
        let delay = Duration::from_millis(reconnect_delay(
            current_attempt,
            self.options.min_delay.as_millis() as u64,
            self.options.max_delay.as_millis() as u64,
        ));
        info!(
            parent: &self.span,
            "Stream factory failure({e}): retrying after {:?}, current_attempt={current_attempt}", delay
        );
        RetryPolicy::WaitRetry(delay, self.seq_no)
    }
}

fn reconnect_delay(attempts: usize, min_delay_millis: u64, max_delay_millis: u64) -> u64 {
    let exp = attempts.saturating_sub(1) as u32;
    std::cmp::min(
        2_u64.saturating_pow(exp) * min_delay_millis,
        max_delay_millis,
    )
}
