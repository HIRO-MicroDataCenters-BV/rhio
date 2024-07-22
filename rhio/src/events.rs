use p2panda_core::{Body, Hash, Header};
use serde::{Deserialize, Serialize};

use crate::extensions::RhioExtensions;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GossipOperation {
    pub body: Body,
    pub header: Header<RhioExtensions>,
}

pub type FileName = String;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Event {
    Create(FileName, Hash),
    Modify,
    Remove,
    Snapshot(Vec<(FileName, Hash)>),
}
