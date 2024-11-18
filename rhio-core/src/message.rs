use anyhow::Result;
use async_nats::{HeaderMap, Message as NatsMessage, Subject};
use bytes::Bytes;
use p2panda_core::{Hash, PrivateKey, PublicKey, Signature};
use rhio_blobs::{BlobHash, BucketName, ObjectKey, ObjectSize};
use serde::{Deserialize, Serialize};

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
            payload: NetworkPayload::NatsMessage(message.subject, message.payload, message.headers),
            signature: None,
        }
    }

    pub fn new_blob_announcement(
        hash: BlobHash,
        bucket_name: BucketName,
        key: ObjectKey,
        size: ObjectSize,
    ) -> Self {
        Self {
            payload: NetworkPayload::BlobAnnouncement(hash, bucket_name, key, size),
            signature: None,
        }
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let message: Self = ciborium::from_reader(bytes)?;
        Ok(message)
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        ciborium::into_writer(&self, &mut bytes).expect("encoding network message");
        bytes
    }

    pub fn hash(&self) -> Hash {
        Hash::new(self.to_bytes())
    }

    pub fn sign(&mut self, private_key: &PrivateKey) {
        self.signature = None;
        let bytes = self.to_bytes();
        let signature = private_key.sign(&bytes);
        self.signature = Some(signature);
    }

    pub fn verify(&self, public_key: &PublicKey) -> bool {
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

#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", content = "value")]
pub enum NetworkPayload {
    #[serde(rename = "blob")]
    BlobAnnouncement(BlobHash, BucketName, ObjectKey, ObjectSize),

    #[serde(rename = "nats")]
    NatsMessage(Subject, Bytes, Option<HeaderMap>),
}

pub fn hash_nats_message(message: &NatsMessage) -> Hash {
    let mut buf = vec![];
    buf.extend_from_slice(message.subject.as_bytes());
    buf.extend_from_slice(b"\r\n");
    if let Some(headers) = &message.headers {
        for (k, vs) in headers.iter() {
            for v in vs.iter() {
                buf.extend_from_slice(k.to_string().as_bytes());
                buf.extend_from_slice(b": ");
                buf.extend_from_slice(v.to_string().as_bytes());
                buf.extend_from_slice(b"\r\n");
            }
        }
        buf.extend_from_slice(b"\r\n");
    }
    buf.extend_from_slice(&message.payload);
    Hash::new(&buf)
}

#[cfg(test)]
mod tests {
    use p2panda_core::PrivateKey;
    use rhio_blobs::BlobHash;

    use super::{NetworkMessage, NetworkPayload};

    #[test]
    fn signature() {
        let private_key = PrivateKey::new();
        let public_key = private_key.public_key();
        let mut header = NetworkMessage {
            payload: NetworkPayload::BlobAnnouncement(
                BlobHash::new(b"test"),
                "my_bucket".into(),
                "path/to/my.file".into(),
                5911,
            ),
            signature: None,
        };
        assert!(!header.verify(&public_key));
        header.sign(&private_key);
        assert!(header.verify(&public_key));
        assert!(!header.verify(&PrivateKey::new().public_key()));
    }
}
