mod bao_file;
mod paths;
mod s3_file;
mod store;
mod utils;

// For blobs we still use BLAKE3 hashes, same as p2panda, but use the implementation of
// `iroh-blobs` instead to ease using their APIs. On top we're getting a different encoding
// (p2panda uses hexadecimal encoding, iroh-blobs uses base32) so we also get a nice "visual
// difference" when logging blob hashes and can distinct them better from public keys or other
// hexadecimal representations.
pub use iroh_blobs::Hash as BlobHash;
use p2panda_core::{PublicKey, Signature};

pub use paths::{Paths, META_SUFFIX, NO_PREFIX, OUTBOARD_SUFFIX};
use serde::{Deserialize, Serialize};
pub use store::S3Store;

/// Name of the S3 bucket which can contain S3 objects stored under keys.
pub type BucketName = String;

/// Size in bytes of the S3 object.
pub type ObjectSize = u64;

/// Key of the S3 object, this is essentially it's "path".
pub type ObjectKey = String;

/// Object in S3 bucket which has not yet been imported to blob store.
///
/// These are objects the user has locally uploaded into the S3 bucket, outside of rhio. We detect
/// these new objects with a special "watcher" service, monitoring the S3 buckets.
#[derive(Clone, Debug)]
pub struct NotImportedObject {
    pub bucket_name: BucketName,
    pub key: ObjectKey,
    pub size: ObjectSize,
}

/// Blobs which have been imported but are still not complete are downloads from remote peers.
///
/// Since we received them from an external source we already know their authentication info, thus
/// they are always signed.
pub type IncompleteBlob = SignedBlobInfo;

/// Completed blobs have either been imported from local S3 objects or downloaded from remote
/// peers.
///
/// For downloaded items we always know the signature, as it has been delivered to us with the blob
/// announcement. For locally uploaded items we don't have the signature as we're signing the blobs
/// when announcing them on the network.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug)]
pub enum CompletedBlob {
    Unsigned(UnsignedBlobInfo),
    Signed(SignedBlobInfo),
}

/// An imported but unsigned blob which indicates that it must be from our local peer.
///
/// As soon as we announce this local blob in the network we sign it but that state does not get
/// persisted in the meta file.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct UnsignedBlobInfo {
    pub hash: BlobHash,
    pub bucket_name: BucketName,
    pub key: ObjectKey,
    pub size: ObjectSize,
}

/// An imported and signed blob which indicates that it must have been downloaded from an remote
/// peer.
#[derive(Clone, Debug)]
pub struct SignedBlobInfo {
    pub hash: BlobHash,
    pub bucket_name: BucketName,
    pub key: ObjectKey,
    pub size: ObjectSize,
    pub public_key: PublicKey,
    pub signature: Signature,
}
