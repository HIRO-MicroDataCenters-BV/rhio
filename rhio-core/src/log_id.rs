use serde::{Deserialize, Serialize};

#[derive(Clone, Default, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct LogId(String);

impl LogId {
    pub fn new(nats_subject: &str) -> Self {
        Self(nats_subject.to_owned())
    }
}
