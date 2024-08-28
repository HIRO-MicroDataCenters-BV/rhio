use p2panda_core::Hash;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct TopicId(pub [u8; 32]);

impl TopicId {
    pub fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub fn from_nats_stream(stream_name: &str, filter_subject: &Option<String>) -> Self {
        let filter_value = match filter_subject {
            Some(filter) => filter.clone(),
            None => "".to_owned(),
        };
        let value = format!("{}{}", stream_name, filter_value);
        Self::from_str(&value)
    }

    pub fn from_str(value: &str) -> Self {
        let hash = Hash::new(value);
        Self(*hash.as_bytes())
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
