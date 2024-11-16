mod bao_file;
mod paths;
mod s3_file;
mod store;

pub use iroh_blobs::Hash as BlobHash;

pub use paths::{Paths, META_SUFFIX, NO_PREFIX, OUTBOARD_SUFFIX};
pub use store::S3Store;

pub type ObjectSize = u64;

pub type ObjectKey = String;

pub type BucketName = String;
