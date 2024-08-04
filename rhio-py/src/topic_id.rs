use p2panda_core::Hash;
use rhio::{messages::ToBytes, topic_id::TopicId as InnerTopicId};

use crate::UniffiCustomTypeConverter;
use crate::error::RhioError;

uniffi::custom_type!(TopicId, Vec<u8>);

#[derive(Clone, Copy, Debug)]
pub struct TopicId {
    inner: InnerTopicId,
}

impl UniffiCustomTypeConverter for TopicId {
    type Builtin = Vec<u8>;

    fn into_custom(val: Self::Builtin) -> uniffi::Result<Self> {
        let topic = TopicId::new(val)?;
        Ok(topic)
    }

    fn from_custom(obj: Self) -> Self::Builtin {
        obj.inner.to_bytes()
    }
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
    pub fn new(bytes: Vec<u8>) -> Result<Self, RhioError> {
        let bytes: [u8; 32] = match TryFrom::try_from(bytes) {
            Ok(bytes) => Ok(bytes),
            Err(_) => Err(anyhow::anyhow!(
                "Failed converting topic bytes with incorrect length"
            )),
        }?;

        Ok(TopicId {
            inner: InnerTopicId::new(bytes),
        })
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
