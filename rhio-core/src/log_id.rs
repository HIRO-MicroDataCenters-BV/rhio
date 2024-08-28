use p2panda_core::PublicKey;
use serde::{Deserialize, Serialize};

#[derive(Clone, Default, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct LogId(String);

impl LogId {
    pub fn new(public_key: &PublicKey, nats_subject: &str) -> Self {
        Self(format!("{}/{}", public_key, nats_subject))
    }
}
