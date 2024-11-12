use std::fmt;

use p2panda_core::{Hash, PublicKey};
use p2panda_net::TopicId;
use p2panda_sync::Topic;
use rhio_core::{Bucket, ScopedBucket, ScopedSubject};
use serde::{Deserialize, Serialize};

use crate::nats::StreamName;

/// Announces interest in certain data from other peers in the network.
#[derive(Clone, Debug, PartialEq, Eq)]
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

/// Shares data from us with other peers in the network.
///
/// Publications join gossip overlays and need to have a topic id for them as well, no sync takes
/// place.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Publication {
    Bucket {
        bucket_name: Bucket,
    },
    Subject {
        stream_name: StreamName,
        subject: ScopedSubject,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum Query {
    Bucket { bucket_name: Bucket },
    Subject { subject: ScopedSubject },
    // @TODO(adz): p2panda's API currently does not allow an explicit way to _not_ sync with other
    // peers. We're using this hacky workaround to indicate in the topic that we're not interested
    // in syncing. The custom rhio sync implementation will check this before and abort the sync
    // process if this value is set.
    NoSync { public_key: PublicKey },
}

impl Query {
    fn prefix(&self) -> &str {
        match self {
            Self::Bucket { .. } => "bucket",
            Self::Subject { .. } => "subject",
            Self::NoSync { .. } => "no-sync",
        }
    }
}

impl From<Subscription> for Query {
    fn from(value: Subscription) -> Self {
        match value {
            Subscription::Bucket { bucket } => Self::Bucket {
                bucket_name: bucket.bucket_name(),
            },
            Subscription::Subject { subject, .. } => Self::Subject { subject },
        }
    }
}

impl From<Publication> for Query {
    fn from(value: Publication) -> Self {
        match value {
            Publication::Bucket { bucket_name } => Self::Bucket { bucket_name },
            Publication::Subject { subject, .. } => Self::Subject { subject },
        }
    }
}

impl fmt::Display for Query {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Query::Bucket { bucket_name } => {
                write!(f, "{}, bucket_name={}", self.prefix(), bucket_name)
            }
            Query::Subject { subject } => {
                write!(f, "{}, subject={}", self.prefix(), subject)
            }
            Query::NoSync { .. } => write!(f, "{}", self.prefix()),
        }
    }
}

/// For sync we're requesting data from this author from this S3 bucket or from this filtered NATS
/// message stream.
impl Topic for Query {}

/// For gossip ("live-mode") we're subscribing to all S3 data from this _bucket name_ or all NATS
/// messages from this _author_.
impl TopicId for Query {
    fn id(&self) -> [u8; 32] {
        let hash = match self {
            Self::Bucket { bucket_name } => Hash::new(format!("{}{}", self.prefix(), bucket_name)),
            Self::Subject { subject, .. } => {
                Hash::new(format!("{}{}", self.prefix(), subject.public_key()))
            }
            Self::NoSync { public_key } => Hash::new(format!("{}{}", self.prefix(), public_key)),
        };

        *hash.as_bytes()
    }
}

#[cfg(test)]
mod tests {
    use p2panda_core::PrivateKey;
    use p2panda_net::TopicId;
    use rhio_core::{ScopedBucket, ScopedSubject};

    use super::{Query, Subscription};

    #[test]
    fn gossip_topic_id() {
        let public_key_1 = PrivateKey::new().public_key();
        let public_key_2 = PrivateKey::new().public_key();

        // Buckets use "bucket name" as gossip topic id.
        let subscription_0: Query = Subscription::Bucket {
            bucket: ScopedBucket::new(public_key_2, "icecreams"),
        }
        .into();
        let subscription_1: Query = Subscription::Bucket {
            bucket: ScopedBucket::new(public_key_1, "icecreams"),
        }
        .into();
        let subscription_2: Query = Subscription::Bucket {
            bucket: ScopedBucket::new(public_key_1, "airplanes"),
        }
        .into();
        assert_eq!(subscription_0.id(), subscription_1.id());
        assert_ne!(subscription_1.id(), subscription_2.id());

        // NATS subjects use public key as gossip topic id.
        let subscription_3: Query = Subscription::Subject {
            stream_name: "data".into(),
            subject: ScopedSubject::new(public_key_1, "*.*.color"),
        }
        .into();
        assert_ne!(subscription_3.id(), subscription_1.id());

        let subscription_4: Query = Subscription::Subject {
            stream_name: "data".into(),
            subject: ScopedSubject::new(public_key_1, "tree.pine.*"),
        }
        .into();
        let subscription_5: Query = Subscription::Subject {
            stream_name: "data".into(),
            subject: ScopedSubject::new(public_key_2, "tree.pine.*"),
        }
        .into();
        assert_eq!(subscription_3.id(), subscription_4.id());
        assert_ne!(subscription_4.id(), subscription_5.id());
    }
}
