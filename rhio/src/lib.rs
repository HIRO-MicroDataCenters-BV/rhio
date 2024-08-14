pub mod actor;
pub mod aggregate;
pub mod config;
pub mod extensions;
pub mod logging;
pub mod messages;
pub mod node;
pub mod operations;
pub mod private_key;
pub mod ticket;
pub mod topic_id;

pub const BLOB_STORE_DIR: &str = "blobs";
pub const BLOB_ANNOUNCE_TOPIC: &str = "rhio/blob_announce";
pub const FILE_SYSTEM_EVENT_TOPIC: &str = "rhio/file_system_sync";
pub const MINIO_ENDPOINT: &str = "http://localhost:9000";
pub const MINIO_REGION: &str = "eu-west-2";
pub const BUCKET_NAME: &str = "rhio";

pub use node::Node;
