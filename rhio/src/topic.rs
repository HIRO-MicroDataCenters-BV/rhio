use p2panda_core::{Hash, PublicKey};
use p2panda_net::TopicId;
use p2panda_sync::Topic;
use rhio_core::{Bucket, ScopedBucket, ScopedSubject};
use serde::{Deserialize, Serialize};

use crate::nats::StreamName;

/// Announces interest in certain data from other peers in the network.
#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum Subscription {
    Bucket {
        bucket: ScopedBucket,
    },
    Subject {
        stream_name: StreamName,
        subject: ScopedSubject,
    },
}

impl Subscription {
    pub fn prefix(&self) -> &str {
        match self {
            Subscription::Bucket { .. } => "bucket",
            Subscription::Subject { .. } => "subject",
        }
    }

    pub fn is_owner(&self, public_key: &PublicKey) -> bool {
        match self {
            Subscription::Bucket { bucket } => bucket.is_owner(public_key),
            Subscription::Subject { subject, .. } => subject.is_owner(public_key),
        }
    }
}

/// For sync we're requesting data from this author from this S3 bucket or from this filtered NATS
/// message stream.
impl Topic for Subscription {}

/// For gossip ("live-mode") we're subscribing to all S3 data from this _bucket name_ or all NATS
/// messages from this _author_.
impl TopicId for Subscription {
    fn id(&self) -> [u8; 32] {
        let hash = match self {
            Subscription::Bucket { bucket } => {
                Hash::new(format!("{}{}", self.prefix(), bucket.bucket_name()))
            }
            Subscription::Subject { subject, .. } => {
                Hash::new(format!("{}{}", self.prefix(), subject.public_key()))
            }
        };

        *hash.as_bytes()
    }
}

/// Shares data from us with other peers in the network.
///
/// Publications join gossip overlays and need to have a topic id for them as well. A `Topic`
/// implementation is not necessary as no sync (catching up on past data) takes place when only
/// publishing.
pub enum Publication {
    Bucket {
        bucket_name: Bucket,
    },
    Subject {
        stream_name: StreamName,
        subject: ScopedSubject,
    },
}

impl Publication {
    pub fn prefix(&self) -> &str {
        match self {
            Publication::Bucket { .. } => "bucket",
            Publication::Subject { .. } => "subject",
        }
    }
}

impl TopicId for Publication {
    fn id(&self) -> [u8; 32] {
        let hash = match self {
            Publication::Bucket { bucket_name } => {
                Hash::new(format!("{}{}", self.prefix(), bucket_name))
            }
            Publication::Subject { subject, .. } => {
                Hash::new(format!("{}{}", self.prefix(), subject.public_key()))
            }
        };

        *hash.as_bytes()
    }
}

#[cfg(test)]
mod tests {
    use p2panda_core::PrivateKey;
    use p2panda_net::TopicId;
    use rhio_core::{ScopedBucket, ScopedSubject};

    use super::Subscription;

    #[test]
    fn gossip_topic_id() {
        let public_key_1 = PrivateKey::new().public_key();
        let public_key_2 = PrivateKey::new().public_key();

        // Buckets use "bucket name" as gossip topic id.
        let subscription_0 = Subscription::Bucket {
            bucket: ScopedBucket::new(public_key_2, "icecreams"),
        };
        let subscription_1 = Subscription::Bucket {
            bucket: ScopedBucket::new(public_key_1, "icecreams"),
        };
        let subscription_2 = Subscription::Bucket {
            bucket: ScopedBucket::new(public_key_1, "airplanes"),
        };
        assert_eq!(subscription_0.id(), subscription_1.id());
        assert_ne!(subscription_1.id(), subscription_2.id());

        // NATS subjects use public key as gossip topic id.
        let subscription_3 = Subscription::Subject {
            stream_name: "data".into(),
            subject: ScopedSubject::new(public_key_1, "*.*.color"),
        };
        assert_ne!(subscription_3.id(), subscription_1.id());

        let subscription_4 = Subscription::Subject {
            stream_name: "data".into(),
            subject: ScopedSubject::new(public_key_1, "tree.pine.*"),
        };
        let subscription_5 = Subscription::Subject {
            stream_name: "data".into(),
            subject: ScopedSubject::new(public_key_2, "tree.pine.*"),
        };
        assert_eq!(subscription_3.id(), subscription_4.id());
        assert_ne!(subscription_4.id(), subscription_5.id());
    }
}
