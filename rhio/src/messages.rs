use std::path::PathBuf;

use anyhow::Result;
use p2panda_core::{Body, Hash, Header, PublicKey};
use serde::{Deserialize, Serialize};

use crate::extensions::RhioExtensions;

#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug)]
pub enum RhioOutEvent<T>
where
    T: Clone,
{
    Ready,
    Message {
        message: T,
        delivered_from: PublicKey,
    },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Message {
    // Sync files in a directory
    FileSystem(FileSystemEvent),

    // Share arbitrary blobs
    BlobAnnouncement(Hash),

    // Application messages
    Application(Vec<u8>),
}

impl Message {
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes: Vec<u8> = Vec::new();
        ciborium::ser::into_writer(&self, &mut bytes).expect("succesfully encodes bytes");
        bytes
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GossipOperation {
    pub message: Message,
    pub header: Header<RhioExtensions>,
}

impl GossipOperation {
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
