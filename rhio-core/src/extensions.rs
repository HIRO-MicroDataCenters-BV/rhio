use p2panda_core::Extension;
use serde::{Deserialize, Serialize};

/// NATS "subject" which are similar to p2panda or Kafka "topics".
///
/// A NATS subject is just a string of characters that form a name the publisher and subscriber can
/// use to find each other. More commonly subject hierarchies are used to scope messages into
/// semantic namespaces.
///
/// Read more: https://docs.nats.io/nats-concepts/subjects
pub type Subject = String;

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct RhioExtensions {
    pub subject: Option<Subject>,
}

impl Extension<Subject> for RhioExtensions {
    fn extract(&self) -> Option<Subject> {
        self.subject.clone()
    }
}
