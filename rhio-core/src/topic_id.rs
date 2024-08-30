use std::str::FromStr;

use p2panda_core::Hash;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct TopicId(pub [u8; 32]);

impl TopicId {
    pub fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }
}

impl FromStr for TopicId {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let hash = Hash::new(value);
        Ok(Self(*hash.as_bytes()))
    }
}

impl From<TopicId> for [u8; 32] {
    fn from(val: TopicId) -> Self {
        val.0
    }
}

impl std::fmt::Display for TopicId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", Hash::from_bytes(self.0).to_hex())
    }
}
