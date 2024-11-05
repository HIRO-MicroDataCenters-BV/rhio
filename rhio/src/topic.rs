use p2panda_core::Hash;
use p2panda_net::TopicId;
use p2panda_sync::Topic;
use rhio_core::{ScopedBucket, ScopedSubject};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum Subscription {
    Bucket(ScopedBucket),
    Subject(ScopedSubject),
}

impl Subscription {
    pub fn prefix(&self) -> &str {
        match self {
            Subscription::Bucket(_) => "bucket",
            Subscription::Subject(_) => "subject",
        }
    }
}

/// For sync we're requesting data from this author from this S3 bucket or from this filtered NATS
/// message stream.
impl Topic for Subscription {}

/// For gossip ("live-mode") we're subscribing to all S3 bucket data or all NATS messages from this
/// author.
impl TopicId for Subscription {
    fn id(&self) -> [u8; 32] {
        let public_key = match self {
            Subscription::Bucket(scoped_bucket) => scoped_bucket.public_key(),
            Subscription::Subject(scoped_subject) => scoped_subject.public_key(),
        };
        *Hash::new(format!("{}{}", self.prefix(), public_key)).as_bytes()
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

        let subscription_1 = Subscription::Bucket(ScopedBucket::new(public_key_1, "icecreams"));
        let subscription_2 = Subscription::Bucket(ScopedBucket::new(public_key_1, "airplanes"));
        assert_eq!(subscription_1.id(), subscription_2.id());

        let subscription_3 = Subscription::Subject(ScopedSubject::new(public_key_1, "*.*.color"));
        assert_ne!(subscription_3.id(), subscription_1.id());

        let subscription_4 = Subscription::Subject(ScopedSubject::new(public_key_1, "tree.pine.*"));
        let subscription_5 = Subscription::Subject(ScopedSubject::new(public_key_2, "tree.pine.*"));
        assert_eq!(subscription_3.id(), subscription_4.id());
        assert_ne!(subscription_4.id(), subscription_5.id());
    }
}
