use p2panda_core::{Body, Hash, Header};
use serde::{Deserialize, Serialize};

use crate::extensions::RhioExtensions;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GossipOperation {
    pub body: Body,
    pub header: Header<RhioExtensions>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Message {
    AnnounceBlob(Hash),
}
