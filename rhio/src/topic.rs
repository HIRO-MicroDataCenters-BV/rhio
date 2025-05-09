use std::{collections::HashSet, fmt};

use p2panda_core::{Hash, PublicKey};
use p2panda_net::TopicId;
use p2panda_sync::Topic;
use rhio_blobs::BucketName;
use rhio_core::{Subject, subjects_to_str};
use serde::{Deserialize, Serialize};

use crate::nats::StreamName;

/// Announces interest in certain data from other peers in the network.
///
/// Subscriptions join gossip overlays and actively try to initiate sync sessions with peers who
/// are subscribed to the same data or publish it.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Subscription {
    Files(FilesSubscription),
    Messages(MessagesSubscription),
}

/// We're interested in blobs from this particular peer ("authorized by it") in their (remote) S3
/// bucket and would like to store it in the given local S3 bucket.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FilesSubscription {
    pub remote_bucket_name: BucketName,
    pub local_bucket_name: BucketName,
    pub public_key: PublicKey,
}

/// We're interested in any NATS message from this particular peer ("authorized by it") for
/// a set of NATS subjects.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MessagesSubscription {
    pub filtered_streams: Vec<FilteredMessageStream>,
    pub public_key: PublicKey,
}

/// Local NATS stream configuration for NATS subjects.
///
/// NATS messages get always published towards our local NATS server without any particular
/// JetStream in mind as soon as we received them from this peer. However, we still need to
/// configure a local stream which is able to give us our current state on this subject, so we
/// effectively sync past NATS messages.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FilteredMessageStream {
    pub subjects: Vec<Subject>,
    // Local JetStream needs to be mentioned to establish state during sync.
    pub stream_name: StreamName,
}

/// Shares data from us with other peers in the network.
///
/// Publications join gossip overlays (need to have a topic id for them as well), but no active
/// sync takes place, they _accept_ sync sessions though for any peer who's subscribed to our data.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Publication {
    /// We're publishing any blob from this given local bucket.
    Files {
        bucket_name: BucketName,
        public_key: PublicKey,
    },

    /// We're publishing any NATS message matching this NATS subjects (wildcards supported) coming
    /// from this local JetStream.
    Messages {
        filtered_stream: FilteredMessageStream,
        public_key: PublicKey,
    },
}

/// Query to announce interest in certain data of a peer to another.
///
/// This is used to guide sync sessions and establish gossip overlays over "topics of interest"
/// among peers.
#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum Query {
    /// We're interested in any blobs from this particular peer ("authorized by it") and bucket.
    Files {
        bucket_name: BucketName,
        public_key: PublicKey,
    },

    /// We're interested in any NATS message from this particular peer ("authorized by it") for
    /// the given NATS subjects.
    Messages {
        subjects: Vec<Subject>,
        public_key: PublicKey,
    },

    // @TODO(adz): p2panda's API currently does not allow an explicit way to _not_ sync with other
    // peers. We're using this hacky workaround to indicate in the topic that we're not interested
    // in syncing. The custom rhio sync implementation will check this before and abort the sync
    // process if this value is set.
    NoSyncFiles {
        public_key: PublicKey,
    },
    NoSyncMessages {
        public_key: PublicKey,
    },
}

// Make sure `p2panda-sync` is happy.
impl Topic for Query {}

impl Query {
    pub fn is_no_sync(&self) -> bool {
        matches!(self, Self::NoSyncFiles { .. } | Self::NoSyncMessages { .. })
    }

    fn prefix(&self) -> &str {
        match self {
            Self::Files { .. } => "bucket",
            Self::Messages { .. } => "subject",
            Self::NoSyncFiles { .. } => "bucket",
            Self::NoSyncMessages { .. } => "subject",
        }
    }
}

impl From<Subscription> for Query {
    fn from(value: Subscription) -> Self {
        match value {
            Subscription::Files(subscription) => Self::Files {
                bucket_name: subscription.remote_bucket_name,
                public_key: subscription.public_key,
            },
            Subscription::Messages(subscription) => {
                // We can be interested in multiple subjects per public key, this is why we merge
                // them all together into one de-duplicated list for the query.
                let mut subjects = HashSet::new();
                for streams in &subscription.filtered_streams {
                    for subject in &streams.subjects {
                        subjects.insert(subject.clone());
                    }
                }

                Self::Messages {
                    subjects: subjects.into_iter().collect(),
                    public_key: subscription.public_key,
                }
            }
        }
    }
}

/// Publications do also form queries, but only to be discoverable on the network for peers who are
/// interested in what we have to offer.
impl From<Publication> for Query {
    fn from(value: Publication) -> Self {
        match value {
            Publication::Files {
                public_key,
                bucket_name,
            } => Self::Files {
                public_key,
                bucket_name,
            },
            Publication::Messages {
                filtered_stream,
                public_key,
                ..
            } => Self::Messages {
                subjects: filtered_stream.subjects,
                public_key,
            },
        }
    }
}

impl fmt::Display for Query {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Query::Files {
                public_key,
                bucket_name,
            } => {
                write!(f, "S3 public_key=\"{}\", bucket_name=\"{bucket_name}\"", {
                    let mut public_key_str = public_key.to_string();
                    public_key_str.truncate(6);
                    public_key_str
                })
            }
            Query::Messages {
                subjects,
                public_key,
                ..
            } => {
                write!(
                    f,
                    "NATS subjects=\"{}\" public_key=\"{}\"",
                    subjects_to_str(subjects.to_owned()),
                    {
                        let mut public_key_str = public_key.to_string();
                        public_key_str.truncate(6);
                        public_key_str
                    }
                )
            }
            Query::NoSyncFiles { .. } => write!(f, "S3 no-sync"),
            Query::NoSyncMessages { .. } => write!(f, "NATS no-sync"),
        }
    }
}

/// For gossip broadcast ("live-mode") we're subscribing to all blobs or all NATS messages from
/// this particular public key / _author_.
impl TopicId for Query {
    fn id(&self) -> [u8; 32] {
        let hash = match self {
            Self::Files { public_key, .. } => Hash::new(format!("{}{}", self.prefix(), public_key)),
            Self::Messages { public_key, .. } => {
                Hash::new(format!("{}{}", self.prefix(), public_key))
            }
            Self::NoSyncFiles { public_key } => {
                Hash::new(format!("{}{}", self.prefix(), public_key))
            }
            Self::NoSyncMessages { public_key } => {
                Hash::new(format!("{}{}", self.prefix(), public_key))
            }
        };

        *hash.as_bytes()
    }
}

/// Returns true if incoming NATS message from that public key is of interest to our local node.
pub fn is_subject_matching(
    subscriptions: &Vec<Subscription>,
    given_subject: &Subject,
    given_public_key: &PublicKey,
) -> bool {
    for subscription in subscriptions {
        match subscription {
            Subscription::Files(_) => continue,
            Subscription::Messages(subscription) => {
                for stream in &subscription.filtered_streams {
                    let is_matching = stream
                        .subjects
                        .iter()
                        .any(|expected_subject| expected_subject.is_matching(given_subject));
                    if is_matching && &subscription.public_key == given_public_key {
                        return true;
                    } else {
                        continue;
                    }
                }
            }
        }
    }
    false
}

/// Returns the subscription info if incoming blob announcement from that public key is of interest
/// to our local node, returns `None` otherwise.
pub fn is_files_subscription_matching<'a>(
    subscriptions: &'a Vec<Subscription>,
    given_public_key: &PublicKey,
    given_bucket_name: &BucketName,
) -> Option<&'a Subscription> {
    for subscription in subscriptions {
        match subscription {
            Subscription::Files(expected) => {
                if &expected.public_key == given_public_key
                    && &expected.remote_bucket_name == given_bucket_name
                {
                    return Some(subscription);
                } else {
                    continue;
                }
            }
            Subscription::Messages(_) => {
                continue;
            }
        }
    }
    None
}

/// Returns the publication info for the blob announcement if we allowed this local bucket to be
/// published in our config, returns `None` otherwise.
pub fn is_bucket_publishable<'a>(
    publications: &'a Vec<Publication>,
    requested_bucket_name: &BucketName,
) -> Option<&'a Publication> {
    for publication in publications {
        match publication {
            Publication::Files { bucket_name, .. } => {
                if bucket_name == requested_bucket_name {
                    return Some(publication);
                } else {
                    continue;
                }
            }
            Publication::Messages { .. } => {
                continue;
            }
        }
    }
    None
}
