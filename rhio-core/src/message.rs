use anyhow::Result;
use async_nats::{HeaderMap, Message as NatsMessage, Subject};
use bytes::Bytes;
use p2panda_core::{Hash, PrivateKey, PublicKey, Signature};
use rhio_blobs::{BlobHash, ObjectKey, ObjectSize};
use serde::{Deserialize, Serialize};

/// Messages which are exchanged in the p2panda network, via sync or gossip.
///
/// They can either represent a) NATS messages or b) Blob announcements. Blobs themselves are
/// exchanged separately via a specialized blob sync protocol.
///
/// Every network message is signed by the _original author_ of the data. That is, the author of
/// the NATS message itself or the author of the to-be-synced blob.
///
/// Peers can further help replicating network messages, even if they are not the original author
/// anymore. They do that by keeping the signature of the original author around for each message.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NetworkMessage {
    /// Actual NATS message or blob announcement data.
    pub payload: NetworkPayload,

    /// Public key of the original author of the NATS message or blob.
    pub public_key: PublicKey,

    /// Cryptographic proof from original author that they are indeed authentic.
    #[serde(skip_serializing_if = "Option::is_none", default = "Option::default")]
    pub signature: Option<Signature>,
}

impl NetworkMessage {
    pub fn new_nats(message: NatsMessage, public_key: &PublicKey) -> Self {
        Self {
            payload: NetworkPayload::NatsMessage(message.subject, message.payload, message.headers),
            public_key: public_key.to_owned(),
            signature: None,
        }
    }

    pub fn new_blob_announcement(
        hash: BlobHash,
        key: ObjectKey,
        size: ObjectSize,
        // @TODO: Add creation timestamp.
        public_key: &PublicKey,
    ) -> Self {
        Self {
            payload: NetworkPayload::BlobAnnouncement(hash, key, size),
            public_key: public_key.to_owned(),
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

    pub fn verify(&self) -> bool {
        match &self.signature {
            Some(signature) => {
                let mut header = self.clone();
                header.signature = None;
                let bytes = header.to_bytes();
                self.public_key.verify(&bytes, signature)
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
    BlobAnnouncement(BlobHash, ObjectKey, ObjectSize),

    #[serde(rename = "nats")]
    NatsMessage(Subject, Bytes, Option<HeaderMap>),
}

// @TODO: Remove this
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
                "path/to/my.file".into(),
                5911,
            ),
            public_key: public_key.clone(),
            signature: None,
        };
        assert!(!header.verify());
        header.sign(&private_key);
        assert!(header.verify());
        header.public_key = PrivateKey::new().public_key();
        assert!(!header.verify());
    }
}
