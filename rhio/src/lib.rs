mod blobs;
pub mod config;
pub mod health;
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

pub(crate) type JoinErrToStr =
    Box<dyn Fn(tokio::task::JoinError) -> String + Send + Sync + 'static>;
