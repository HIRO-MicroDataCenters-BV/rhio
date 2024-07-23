use anyhow::Result;
use p2panda_core::Hash;
use serde::{Deserialize, Serialize};

pub type FileName = String;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum FileSystemEvent {
    Create(FileName, Hash),
    Modify,
    Remove,
    Snapshot(Vec<(FileName, Hash)>),
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
