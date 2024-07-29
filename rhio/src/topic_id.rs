use p2panda_core::hash::Hash;
use p2panda_net::TopicId as InnerTopicId;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct TopicId(InnerTopicId);

impl TopicId {
    pub fn from_str(str: &str) -> Self {
        let hash = Hash::new(str);
        Self(*hash.as_bytes())
    }
}

impl Into<InnerTopicId> for TopicId {
    fn into(self) -> InnerTopicId {
        self.0
    }
}

impl ToString for TopicId {
    fn to_string(&self) -> String {
        Hash::from_bytes(self.0).to_hex()
    }
}
