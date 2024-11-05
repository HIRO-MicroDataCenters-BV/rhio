use std::fmt;
use std::str::FromStr;

use anyhow::{anyhow, bail};
use p2panda_core::PublicKey;
use serde::{Deserialize, Serialize};

const KEY_PREFIX_TOKEN: &str = "/";

pub type Bucket = String;

/// Special S3 bucket name for the "rhio" application which holds a regular S3 bucket name next to
/// a peer's Ed25519 public key.
///
/// This makes the S3 bucket name "scoped" to this particular identity.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ScopedBucket(PublicKey, Bucket);

impl ScopedBucket {
    pub fn new(public_key: PublicKey, bucket_name: &str) -> Self {
        Self(public_key, bucket_name.to_string())
    }

    pub fn public_key(&self) -> PublicKey {
        self.0
    }

    pub fn bucket_name(&self) -> Bucket {
        self.1.clone()
    }
}

impl fmt::Display for ScopedBucket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}{}{}", self.0, KEY_PREFIX_TOKEN, self.bucket_name())
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

        let public_key = PublicKey::from_str(parts[0])
            .map_err(|err| anyhow!("invalid public key in rhio subject: {err}"))?;

        Ok(Self(public_key, parts[1].to_string()))
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
