#![feature(assert_matches)]

pub mod actor;
pub mod aggregate;
pub mod config;
pub mod extensions;
pub mod logging;
pub mod messages;
pub mod node;
pub mod operations;
pub mod private_key;
pub mod topic_id;
pub mod ticket;

pub const BLOB_ANNOUNCE_TOPIC: &str = "rhio/blob_announce";
pub const FILE_SYSTEM_EVENT_TOPIC: &str = "rhio/file_system_event";

pub use node::Node;