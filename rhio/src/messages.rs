use std::path::PathBuf;

use anyhow::Result;
use p2panda_core::{Body, Hash, Header, PublicKey};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::extensions::RhioExtensions;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MessageMeta {
    pub operation_timestamp: u64,
    pub received_at: u64,
    pub delivered_from: PublicKey,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Message<T> {
    // Sync files in a directory
    FileSystem(FileSystemEvent),

    // Share arbitrary blobs
    BlobAnnouncement(Hash),

    // Application messages
    Application(T),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GossipOperation<T> {
    pub message: Message<T>,
    pub header: Header<RhioExtensions>,
}

impl<T> GossipOperation<T>
where
    T: Serialize + DeserializeOwned + Clone,
{
    pub fn body(&self) -> Body {
        Body::new(&self.message.to_bytes())
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum FileSystemEvent {
    Create(PathBuf, Hash),
    Modify,
    Remove,
    Snapshot(Vec<(PathBuf, Hash)>),
}

pub trait ToBytes {
    fn to_bytes(&self) -> Vec<u8>;
}

pub trait FromBytes<T> {
    fn from_bytes(bytes: &[u8]) -> Result<T>;
}

impl<T: Serialize> ToBytes for T {
    fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        ciborium::into_writer(&self, &mut bytes).expect("type can be serialized");
        bytes
    }
}

impl<T: DeserializeOwned> FromBytes<T> for T {
    fn from_bytes(bytes: &[u8]) -> Result<T> {
        let value = ciborium::from_reader(bytes)?;
        Ok(value)
    }
}
