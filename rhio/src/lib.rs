mod blobs;
mod config;
mod nats;
mod network;
mod node;
mod topic;
mod tracing;

pub(crate) type JoinErrToStr =
    Box<dyn Fn(tokio::task::JoinError) -> String + Send + Sync + 'static>;
