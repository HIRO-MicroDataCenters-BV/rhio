use p2panda_core::hash::Hash;
use p2panda_net::TopicId as InnerTopicId;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct TopicId(pub InnerTopicId);

impl TopicId {
    pub fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }
    pub fn new_from_str(str: &str) -> Self {
        let hash = Hash::new(str);
        Self(*hash.as_bytes())
    }
}

impl From<TopicId> for InnerTopicId {
    fn from(val: TopicId) -> Self {
        val.0
    }
}

impl std::fmt::Display for TopicId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", Hash::from_bytes(self.0).to_hex())
    }
}
