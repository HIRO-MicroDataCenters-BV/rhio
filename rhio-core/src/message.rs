use anyhow::Result;
use async_nats::Message as NatsMessage;
use p2panda_core::{PrivateKey, PublicKey, Signature};
use serde::{Deserialize, Serialize};

use crate::ScopedBucket;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NetworkMessage {
    #[serde(rename = "p")]
    pub payload: NetworkPayload,

    #[serde(
        rename = "s",
        skip_serializing_if = "Option::is_none",
        default = "Option::default"
    )]
    pub signature: Option<Signature>,
}

impl NetworkMessage {
    pub fn new_nats(message: NatsMessage) -> Self {
        Self {
            payload: NetworkPayload::NatsMessage(message),
            signature: None,
        }
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let message: Self = ciborium::from_reader(&bytes[..])?;
        Ok(message)
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        ciborium::into_writer(&self, &mut bytes).expect("encoding header");
        bytes
    }

    pub fn sign(&mut self, private_key: &PrivateKey) {
        self.signature = None;
        let bytes = self.to_bytes();
        let signature = private_key.sign(&bytes);
        self.signature = Some(signature);
    }

    pub fn verify(&mut self, public_key: &PublicKey) -> bool {
        match &self.signature {
            Some(signature) => {
                let mut header = self.clone();
                header.signature = None;
                let bytes = header.to_bytes();
                public_key.verify(&bytes, signature)
            }
            None => false,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", content = "value")]
pub enum NetworkPayload {
    #[serde(rename = "blob")]
    BlobAnnouncement(ScopedBucket),

    #[serde(rename = "nats")]
    NatsMessage(NatsMessage),
}

#[cfg(test)]
mod tests {
    use p2panda_core::PrivateKey;

    use crate::ScopedBucket;

    use super::{NetworkMessage, NetworkPayload};

    #[test]
    fn signature() {
        let private_key = PrivateKey::new();
        let public_key = private_key.public_key();
        let mut header = NetworkMessage {
            payload: NetworkPayload::BlobAnnouncement(ScopedBucket::new(
                public_key,
                "my_bucket".into(),
            )),
            signature: None,
        };
        println!("{}", hex::encode(header.to_bytes()));
        assert!(!header.verify(&public_key));
        header.sign(&private_key);
        assert!(header.verify(&public_key));
        assert!(!header.verify(&PrivateKey::new().public_key()));
    }
}
