use p2panda_core::{Extension, Hash};
use p2panda_engine::extensions::{PruneFlag, StreamName};
use serde::{Deserialize, Serialize};

use crate::LogId;

/// NATS "subject" which are similar to p2panda or Kafka "topics".
///
/// A NATS subject is just a string of characters that form a name the publisher and subscriber can
/// use to find each other. More commonly subject hierarchies are used to scope messages into
/// semantic namespaces.
///
/// Read more: https://docs.nats.io/nats-concepts/subjects
pub type Subject = String;

pub type TopicId = [u8; 32];

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct RhioExtensions {
    /// Mandatory field containing the NATS subject.
    #[serde(rename = "s")]
    pub subject: Option<Subject>,

    /// Optional field for messages which announce new blobs in the network, identified by this
    /// hash. p2panda peers will connect to other nodes and replicate the blob on receipt.
    #[serde(rename = "b", skip_serializing_if = "Option::is_none")]
    pub blob_hash: Option<Hash>,

    /// Optional field signifying that all operations in an authors' log prior to this one can be
    /// pruned. This comes with the assumption that all state which could be lost through removing
    /// operations is contained in this operations' body.
    #[serde(rename = "p", skip_serializing_if = "Option::is_none")]
    pub prune_flag: Option<PruneFlag>,
}

impl Extension<Subject> for RhioExtensions {
    fn extract(&self) -> Option<Subject> {
        self.subject.clone()
    }
}

impl Extension<LogId> for RhioExtensions {
    fn extract(&self) -> Option<LogId> {
        self.subject.clone().map(|subject| LogId::new(&subject))
    }
}

impl Extension<Hash> for RhioExtensions {
    fn extract(&self) -> Option<Hash> {
        self.blob_hash
    }
}

impl Extension<StreamName> for RhioExtensions {
    fn extract(&self) -> Option<StreamName> {
        None
    }
}

impl Extension<PruneFlag> for RhioExtensions {
    fn extract(&self) -> Option<PruneFlag> {
        self.prune_flag
            .clone()
            .or_else(|| Some(PruneFlag::default()))
    }
}
