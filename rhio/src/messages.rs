use std::path::PathBuf;

use anyhow::Result;
use p2panda_core::{Body, Hash, Header, PublicKey};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::extensions::RhioExtensions;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MessageContext {
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

impl<T> Message<T>
where
    T: Serialize + DeserializeOwned + Clone,
{
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes: Vec<u8> = Vec::new();
        ciborium::ser::into_writer(&self, &mut bytes).expect("succesfully encodes bytes");
        bytes
    }
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

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes: Vec<u8> = Vec::new();
        ciborium::ser::into_writer(&(&self), &mut bytes).expect("succesfully encodes bytes");
        bytes
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let operation = ciborium::from_reader::<Self, _>(bytes)?;
        Ok(operation)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum FileSystemEvent {
    Create(PathBuf, Hash),
    Modify,
    Remove,
    Snapshot(Vec<(PathBuf, Hash)>),
}

impl FileSystemEvent {
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let event = ciborium::from_reader::<FileSystemEvent, _>(bytes)?;
        Ok(event)
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes: Vec<u8> = Vec::new();
        ciborium::ser::into_writer(&self, &mut bytes).expect("succesfully encodes bytes");
        bytes
    }
}
