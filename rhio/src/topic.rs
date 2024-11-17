use std::fmt;

use p2panda_core::{Hash, PublicKey};
use p2panda_net::TopicId;
use p2panda_sync::Topic;
use rhio_blobs::BucketName;
use rhio_core::Subject;
use serde::{Deserialize, Serialize};

use crate::nats::StreamName;

/// Announces interest in certain data from other peers in the network.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Subscription {
    Bucket {
        bucket_name: BucketName,
        public_key: PublicKey,
    },
    Subject {
        subject: Subject,
        stream_name: StreamName,
        public_key: PublicKey,
    },
}

impl Subscription {
    pub fn prefix(&self) -> &str {
        match self {
            Subscription::Bucket { .. } => "bucket",
            Subscription::Subject { .. } => "subject",
        }
    }
}

/// Shares data from us with other peers in the network.
///
/// Publications join gossip overlays and need to have a topic id for them as well, no sync takes
/// place.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Publication {
    Bucket {
        bucket_name: BucketName,
        public_key: PublicKey,
    },
    Subject {
        stream_name: StreamName,
        subject: Subject,
        public_key: PublicKey,
    },
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum Query {
    Bucket {
        bucket_name: BucketName,
        public_key: PublicKey,
    },
    Subject {
        subject: Subject,
        public_key: PublicKey,
    },
    // @TODO(adz): p2panda's API currently does not allow an explicit way to _not_ sync with other
    // peers. We're using this hacky workaround to indicate in the topic that we're not interested
    // in syncing. The custom rhio sync implementation will check this before and abort the sync
    // process if this value is set.
    NoSyncBucket {
        public_key: PublicKey,
    },
    NoSyncSubject {
        public_key: PublicKey,
    },
}

impl Query {
    pub fn public_key(&self) -> &PublicKey {
        match self {
            Query::Bucket { public_key, .. } => public_key,
            Query::Subject { public_key, .. } => public_key,
            Query::NoSyncBucket { public_key } => public_key,
            Query::NoSyncSubject { public_key } => public_key,
        }
    }

    pub fn is_no_sync(&self) -> bool {
        match self {
            Self::NoSyncBucket { .. } | Self::NoSyncSubject { .. } => true,
            _ => false,
        }
    }

    fn prefix(&self) -> &str {
        match self {
            Self::Bucket { .. } => "bucket",
            Self::Subject { .. } => "subject",
            Self::NoSyncBucket { .. } => "bucket",
            Self::NoSyncSubject { .. } => "subject",
        }
    }
}

impl From<Subscription> for Query {
    fn from(value: Subscription) -> Self {
        match value {
            Subscription::Bucket {
                bucket_name,
                public_key,
            } => Self::Bucket {
                bucket_name,
                public_key,
            },
            Subscription::Subject {
                subject,
                public_key,
                ..
            } => Self::Subject {
                subject,
                public_key,
            },
        }
    }
}

impl From<Publication> for Query {
    fn from(value: Publication) -> Self {
        match value {
            Publication::Bucket {
                bucket_name,
                public_key,
            } => Self::Bucket {
                bucket_name,
                public_key,
            },
            Publication::Subject {
                subject,
                public_key,
                ..
            } => Self::Subject {
                subject,
                public_key,
            },
        }
    }
}

impl fmt::Display for Query {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Query::Bucket { bucket_name, .. } => {
                write!(f, "s3 bucket={}", bucket_name)
            }
            Query::Subject { subject, .. } => {
                write!(f, "nats subject={}", subject)
            }
            Query::NoSyncBucket { .. } => write!(f, "no-sync"),
            Query::NoSyncSubject { .. } => write!(f, "no-sync"),
        }
    }
}

/// For sync we're requesting data from this author from this S3 bucket or from this filtered NATS
/// message stream.
impl Topic for Query {}

/// For gossip ("live-mode") we're subscribing to all S3 data or all NATS messages from this
/// _author_.
impl TopicId for Query {
    fn id(&self) -> [u8; 32] {
        let hash = match self {
            Self::Bucket { public_key, .. } => {
                Hash::new(format!("{}{}", self.prefix(), public_key))
            }
            Self::Subject { public_key, .. } => {
                Hash::new(format!("{}{}", self.prefix(), public_key))
            }
            Self::NoSyncBucket { public_key } => {
                Hash::new(format!("{}{}", self.prefix(), public_key))
            }
            Self::NoSyncSubject { public_key } => {
                Hash::new(format!("{}{}", self.prefix(), public_key))
            }
        };

        *hash.as_bytes()
    }
}

/// Returns true if incoming NATS message from that public key is of interest to our local node.
pub fn is_subject_matching(
    subscriptions: &Vec<Subscription>,
    incoming: &Subject,
    delivered_from: &PublicKey,
) -> bool {
    for subscription in subscriptions {
        match subscription {
            Subscription::Bucket { .. } => continue,
            Subscription::Subject {
                subject,
                public_key,
                ..
            } => {
                if subject.is_matching(incoming) && public_key == delivered_from {
                    return true;
                } else {
                    continue;
                }
            }
        }
    }
    false
}

/// Returns true if incoming blob announcement from that public key is of interest to our local
/// node.
pub fn is_bucket_matching(
    subscriptions: &Vec<Subscription>,
    incoming: &BucketName,
    delivered_from: &PublicKey,
) -> bool {
    for subscription in subscriptions {
        match subscription {
            Subscription::Bucket {
                bucket_name,
                public_key,
            } => {
                if bucket_name == incoming && public_key == delivered_from {
                    return true;
                } else {
                    continue;
                }
            }
            Subscription::Subject { .. } => {
                continue;
            }
        }
    }
    false
}

/// Returns the publication info for the blob announcement if it exists in our config, returns
/// `None` otherwise.
pub fn is_bucket_publishable<'a>(
    publications: &'a Vec<Publication>,
    outgoing: &BucketName,
) -> Option<&'a Publication> {
    for publication in publications {
        match publication {
            Publication::Bucket { bucket_name, .. } => {
                if bucket_name == outgoing {
                    return Some(publication);
                } else {
                    continue;
                }
            }
            Publication::Subject { .. } => {
                continue;
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use p2panda_core::PrivateKey;
    use p2panda_net::TopicId;

    use super::{Query, Subscription};

    #[test]
    fn gossip_topic_id() {
        let public_key_1 = PrivateKey::new().public_key();
        let public_key_2 = PrivateKey::new().public_key();

        // Buckets use "bucket name" as gossip topic id.
        let subscription_0: Query = Subscription::Bucket {
            bucket_name: "icecreams".into(),
            public_key: public_key_2,
        }
        .into();
        let subscription_1: Query = Subscription::Bucket {
            bucket_name: "icecreams".into(),
            public_key: public_key_1,
        }
        .into();
        let subscription_2: Query = Subscription::Bucket {
            bucket_name: "airplanes".into(),
            public_key: public_key_1,
        }
        .into();
        assert_ne!(subscription_0.id(), subscription_1.id());
        assert_eq!(subscription_1.id(), subscription_2.id());

        // NATS subjects use public key as gossip topic id.
        let subscription_3: Query = Subscription::Subject {
            stream_name: "data".into(),
            subject: "*.*.color".parse().unwrap(),
            public_key: public_key_1,
        }
        .into();
        assert_ne!(subscription_3.id(), subscription_1.id());

        let subscription_4: Query = Subscription::Subject {
            stream_name: "data".into(),
            subject: "tree.pine.*".parse().unwrap(),
            public_key: public_key_1,
        }
        .into();
        let subscription_5: Query = Subscription::Subject {
            stream_name: "data".into(),
            subject: "tree.pine.*".parse().unwrap(),
            public_key: public_key_2,
        }
        .into();
        assert_eq!(subscription_3.id(), subscription_4.id());
        assert_ne!(subscription_4.id(), subscription_5.id());
    }
}
