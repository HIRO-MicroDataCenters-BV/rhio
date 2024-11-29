use anyhow::{anyhow, Context, Result};
use async_nats::{HeaderMap, Message as NatsMessage, Subject};
use bytes::Bytes;
use p2panda_core::{Hash, PrivateKey, PublicKey, Signature};
use rhio_blobs::{BlobHash, BucketName, ObjectKey, ObjectSize, SignedBlobInfo};
use serde::{Deserialize, Serialize};

use crate::nats::{remove_custom_nats_headers, NATS_RHIO_PUBLIC_KEY, NATS_RHIO_SIGNATURE};

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

    pub fn new_signed_nats(message: NatsMessage) -> Result<Self> {
        let Some(headers) = &message.headers else {
            return Err(anyhow!("no headers given in NATS message"));
        };

        let signature: Signature = {
            let bytes = hex::decode(
                headers
                    .get(NATS_RHIO_SIGNATURE)
                    .ok_or(anyhow!("no signature given in NATS message"))?,
            )
            .context("decode signature in NATS message")?;
            Signature::try_from(&bytes[..]).context("parse signature in NATS message")?
        };

        let public_key: PublicKey = headers
            .get(NATS_RHIO_PUBLIC_KEY)
            .ok_or(anyhow!("no public key given in NATS message"))?
            .to_string()
            .parse()?;

        Ok(Self {
            payload: NetworkPayload::NatsMessage(
                message.subject,
                message.payload,
                remove_custom_nats_headers(headers),
            ),
            public_key,
            signature: Some(signature),
        })
    }

    pub fn new_blob_announcement(
        hash: BlobHash,
        bucket_name: BucketName,
        key: ObjectKey,
        size: ObjectSize,
        public_key: &PublicKey,
    ) -> Self {
        Self {
            payload: NetworkPayload::BlobAnnouncement(hash, bucket_name, key, size),
            public_key: public_key.to_owned(),
            signature: None,
        }
    }

    pub fn new_signed_blob_announcement(signed_blob: SignedBlobInfo) -> Self {
        Self {
            payload: NetworkPayload::BlobAnnouncement(
                signed_blob.hash,
                signed_blob.remote_bucket_name,
                signed_blob.key,
                signed_blob.size,
            ),
            public_key: signed_blob.public_key,
            signature: Some(signed_blob.signature),
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
                let mut message = self.clone();

                // Remove existing signature.
                message.signature = None;

                // Remove potentially existing custom NATS headers for rhio which contain signature
                // and public key.
                if let NetworkPayload::NatsMessage(subject, payload, Some(headers)) =
                    &message.payload
                {
                    message.payload = NetworkPayload::NatsMessage(
                        subject.to_owned(),
                        payload.to_owned(),
                        remove_custom_nats_headers(headers),
                    );
                }

                let bytes = message.to_bytes();
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
    BlobAnnouncement(BlobHash, BucketName, ObjectKey, ObjectSize),

    #[serde(rename = "nats")]
    NatsMessage(Subject, Bytes, Option<HeaderMap>),
}

#[cfg(test)]
mod tests {
    use async_nats::{HeaderMap, Message as NatsMessage};
    use p2panda_core::PrivateKey;
    use rhio_blobs::BlobHash;

    use crate::{NATS_RHIO_PUBLIC_KEY, NATS_RHIO_SIGNATURE};

    use super::{NetworkMessage, NetworkPayload};

    #[test]
    fn signature() {
        let private_key = PrivateKey::new();
        let public_key = private_key.public_key();
        let mut message = NetworkMessage {
            payload: NetworkPayload::BlobAnnouncement(
                BlobHash::new(b"test"),
                "bucket-out".into(),
                "path/to/my.file".into(),
                5911,
            ),
            public_key,
            signature: None,
        };
        assert!(!message.verify());
        message.sign(&private_key);
        assert!(message.verify());
        message.public_key = PrivateKey::new().public_key();
        assert!(!message.verify());
    }

    #[test]
    fn nats_signatures() {
        let private_key = PrivateKey::new();
        let public_key = private_key.public_key();

        // Encode and sign NATS message wrapped in network message.
        let mut nats_message = NatsMessage {
            subject: "foo.meta".into(),
            reply: None,
            payload: vec![1, 2, 3].into(),
            headers: None,
            status: None,
            description: None,
            length: 0,
        };
        let mut message = NetworkMessage::new_nats(nats_message.clone(), &public_key);
        message.sign(&private_key);
        let hash = message.hash();

        // Add authentication data to NATS message itself.
        nats_message.headers = Some({
            let mut headers = HeaderMap::new();
            let signature = message.signature.unwrap().to_string();
            let public_key = message.public_key.to_string();
            headers.insert(NATS_RHIO_SIGNATURE, signature);
            headers.insert(NATS_RHIO_PUBLIC_KEY, public_key);
            headers
        });

        // Re-create network message from NATS message and check signature.
        let message = NetworkMessage::new_signed_nats(nats_message).unwrap();
        assert!(message.verify());
        assert_eq!(hash, message.hash());
    }
}
