use std::collections::HashMap;

use p2panda_store::TopicMap;
use serde::{Deserialize, Serialize};

#[derive(Clone, Default, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct LogId(String);

impl LogId {
    pub fn new(nats_subject: &str) -> Self {
        Self(nats_subject.to_owned())
    }
}

#[derive(Clone, Default, Debug)]
pub struct RhioTopicMap(HashMap<[u8; 32], LogId>);

impl RhioTopicMap {
    pub fn insert(&mut self, topic_id: [u8; 32], log_id: LogId) {
        self.0.insert(topic_id, log_id);
    }
}

impl TopicMap<[u8; 32], LogId> for RhioTopicMap {
    fn get(&self, topic: &[u8; 32]) -> Option<LogId> {
        self.0.get(topic).cloned()
    }
}
