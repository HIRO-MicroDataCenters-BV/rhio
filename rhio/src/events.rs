use p2panda_core::{Body, Hash, Header, Operation};
use serde::{Deserialize, Serialize};

use crate::extensions::RhioExtensions;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GossipOperation {
    pub body: Body,
    pub header: Header<RhioExtensions>,
}

impl Into<Operation<RhioExtensions>> for GossipOperation {
    fn into(self) -> Operation<RhioExtensions> {
        Operation {
            hash: self.header.hash(),
            header: self.header,
            body: Some(self.body),
        }
    }
}

impl From<Operation<RhioExtensions>> for GossipOperation {
    fn from(value: Operation<RhioExtensions>) -> Self {
        GossipOperation {
            body: value.body.expect("all gossip operations have a body"),
            header: value.header,
        }
    }
}

pub type FileName = String;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Event {
    Create(FileName, Hash),
    Modify,
    Remove,
    Snapshot(Vec<(FileName, Hash)>),
}
