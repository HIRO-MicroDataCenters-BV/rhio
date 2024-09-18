use p2panda_store::TopicMap;
use serde::{Deserialize, Serialize};

#[derive(Clone, Default, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct LogId(String);

impl LogId {
    pub fn new(nats_subject: &str) -> Self {
        Self(nats_subject.to_owned())
    }
}

#[derive(Clone, Debug)]
pub struct RhioTopicMap {}

impl TopicMap<[u8; 32], [u8; 32]> for RhioTopicMap {
    fn get(&self, topic: &[u8; 32]) -> Option<[u8; 32]> {
        Some(topic.clone())
    }
}
