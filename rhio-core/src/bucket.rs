use std::fmt;
use std::str::FromStr;

use anyhow::{anyhow, bail};
use p2panda_core::PublicKey;
use rhio_blobs::BucketName;
use serde::{Deserialize, Serialize};

const KEY_PREFIX_TOKEN: &str = "/";

/// Special S3 bucket name for the "rhio" application which holds a regular S3 bucket name next to
/// a peer's Ed25519 public key.
///
/// This makes the S3 bucket name "scoped" to this particular identity.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ScopedBucket(BucketName, PublicKey);

impl ScopedBucket {
    pub fn new(bucket_name: &str, public_key: PublicKey) -> Self {
        Self(bucket_name.to_string(), public_key)
    }

    pub fn public_key(&self) -> PublicKey {
        self.1
    }

    pub fn bucket_name(&self) -> BucketName {
        self.0.clone()
    }

    pub fn is_owner(&self, public_key: &PublicKey) -> bool {
        &self.1 == public_key
    }
}

impl fmt::Display for ScopedBucket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}{}{}",
            self.bucket_name(),
            KEY_PREFIX_TOKEN,
            self.public_key()
        )
    }
}

impl FromStr for ScopedBucket {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let parts: Vec<&str> = value.split(KEY_PREFIX_TOKEN).collect();
        if parts.len() != 2 {
            bail!(
                "needs to have exactly one {} in rhio subject",
                KEY_PREFIX_TOKEN
            );
        }

        let public_key = PublicKey::from_str(parts[1])
            .map_err(|err| anyhow!("invalid public key in rhio subject: {err}"))?;

        Ok(Self(parts[0].to_string(), public_key))
    }
}

impl Serialize for ScopedBucket {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for ScopedBucket {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value: String = String::deserialize(deserializer)?;
        Self::from_str(&value).map_err(|err| serde::de::Error::custom(err.to_string()))
    }
}
