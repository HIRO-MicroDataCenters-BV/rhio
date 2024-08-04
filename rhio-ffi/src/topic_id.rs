use p2panda_core::Hash;
use rhio::topic_id::TopicId as InnerTopicId;

#[derive(Clone, Copy, Debug, uniffi::Object)]
pub struct TopicId {
    inner: InnerTopicId,
}

impl From<InnerTopicId> for TopicId {
    fn from(value: InnerTopicId) -> Self {
        Self { inner: value }
    }
}

impl Into<InnerTopicId> for TopicId {
    fn into(self) -> InnerTopicId {
        self.inner
    }
}

#[uniffi::export]
impl TopicId {
    #[uniffi::constructor]
    pub fn new(bytes: &Vec<u8>) -> Self {
        let bytes: [u8; 32] =
            TryFrom::try_from(bytes.to_owned()).expect("incorrect topic bytes length");
        Self {
            inner: InnerTopicId::new(bytes),
        }
    }

    #[uniffi::constructor]
    pub fn new_from_str(topic_str: &str) -> Self {
        Self {
            inner: InnerTopicId::new_from_str(topic_str),
        }
    }
}

impl std::fmt::Display for TopicId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", Hash::from_bytes(self.inner.into()).to_hex())
    }
}
