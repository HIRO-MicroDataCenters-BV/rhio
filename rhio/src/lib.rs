pub mod actor;
pub mod aggregate;
pub mod config;
pub mod extensions;
pub mod logging;
pub mod messages;
pub mod operations;
pub mod node;
pub mod private_key;

// @TODO: Use real topic id
const TOPIC_ID: p2panda_net::TopicId = [1; 32];

pub use node::Node;
