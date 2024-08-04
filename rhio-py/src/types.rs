use std::path::PathBuf;
use std::str::FromStr;
use std::net::SocketAddr as InnerSocketAddr;

use p2panda_core::{Hash as InnerHash, PublicKey as InnerPublicKey};

use crate::UniffiCustomTypeConverter;

uniffi::custom_type!(Hash, String);

#[derive(Debug, Clone)]
pub struct Hash {
    pub inner: InnerHash,
}

impl UniffiCustomTypeConverter for Hash {
    type Builtin = String;

    fn into_custom(val: Self::Builtin) -> uniffi::Result<Self> {
        let hash = InnerHash::from_str(&val)?;
        Ok(Self { inner: hash })
    }

    fn from_custom(obj: Self) -> Self::Builtin {
        obj.inner.to_string()
    }
}

impl From<InnerHash> for Hash {
    fn from(value: InnerHash) -> Self {
        Self { inner: value }
    }
}

impl Into<InnerHash> for Hash {
    fn into(self) -> InnerHash {
        self.inner
    }
}

uniffi::custom_type!(Path, String);

#[derive(Debug, Clone)]
pub struct Path {
    inner: PathBuf,
}

impl UniffiCustomTypeConverter for Path {
    type Builtin = String;

    fn into_custom(val: Self::Builtin) -> uniffi::Result<Self> {
        let path = PathBuf::from_str(&val)?;
        Ok(Self { inner: path })
    }

    fn from_custom(obj: Self) -> Self::Builtin {
        obj.inner
            .to_str()
            .expect("is valid UTF-8 string")
            .to_string()
    }
}

impl From<PathBuf> for Path {
    fn from(value: PathBuf) -> Self {
        Self { inner: value }
    }
}

impl Into<PathBuf> for Path {
    fn into(self) -> PathBuf {
        self.inner
    }
}

uniffi::custom_type!(PublicKey, String);

#[derive(Debug, Clone)]
pub struct PublicKey {
    pub inner: InnerPublicKey,
}

impl UniffiCustomTypeConverter for PublicKey {
    type Builtin = String;

    fn into_custom(val: Self::Builtin) -> uniffi::Result<Self> {
        let public_key = InnerPublicKey::from_str(&val)?;
        Ok(Self { inner: public_key })
    }

    fn from_custom(obj: Self) -> Self::Builtin {
        obj.inner.to_string()
    }
}

impl From<InnerPublicKey> for PublicKey {
    fn from(value: InnerPublicKey) -> Self {
        Self { inner: value }
    }
}

impl Into<InnerPublicKey> for PublicKey {
    fn into(self) -> InnerPublicKey {
        self.inner
    }
}

uniffi::custom_type!(SocketAddr, String);

#[derive(Debug, Clone)]
pub struct SocketAddr {
    pub inner: InnerSocketAddr,
}

impl UniffiCustomTypeConverter for SocketAddr {
    type Builtin = String;

    fn into_custom(val: Self::Builtin) -> uniffi::Result<Self> {
        let addr = InnerSocketAddr::from_str(&val)?;
        Ok(Self { inner: addr })
    }

    fn from_custom(obj: Self) -> Self::Builtin {
        obj.inner.to_string()
    }
}

impl From<InnerSocketAddr> for SocketAddr {
    fn from(value: InnerSocketAddr) -> Self {
        Self { inner: value }
    }
}

impl Into<InnerSocketAddr> for SocketAddr {
    fn into(self) -> InnerSocketAddr {
        self.inner
    }
}
