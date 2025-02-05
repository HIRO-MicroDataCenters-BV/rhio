mod blobs;
pub mod context;
pub mod context_builder;
mod http;
pub mod metrics;
mod nats;
mod network;
mod node;
mod topic;
pub mod tracing;

pub use nats::StreamName;
pub use topic::{
    FilesSubscription, FilteredMessageStream, MessagesSubscription, Publication, Subscription,
};

pub use node::rhio::Node;

pub mod built_info {
    // The file has been placed there by the build script.
    include!(concat!(env!("OUT_DIR"), "/built.rs"));
}

#[cfg(test)]
mod tests;

pub(crate) type JoinErrToStr =
    Box<dyn Fn(tokio::task::JoinError) -> String + Send + Sync + 'static>;
