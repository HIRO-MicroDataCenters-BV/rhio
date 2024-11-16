pub mod bao_file;
pub mod paths;
pub mod s3_file;
mod store;

pub use iroh_blobs::Hash as BlobHash;

pub use store::S3Store;

pub type ObjectSize = u64;

pub type ObjectKey = String;

pub type BucketName = String;
