use std::net::SocketAddr as InnerSocketAddr;
use std::path::PathBuf;
use std::str::FromStr;

use p2panda_core::{Hash as InnerHash, PublicKey as InnerPublicKey};
use rhio::config::{parse_import_path, ImportPath as InnerImportPath};

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

impl From<Hash> for InnerHash {
    fn from(value: Hash) -> Self {
        value.inner
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

impl From<Path> for PathBuf {
    fn from(value: Path) -> Self {
        value.inner
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

impl From<PublicKey> for InnerPublicKey {
    fn from(value: PublicKey) -> Self {
        value.inner
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

impl From<SocketAddr> for InnerSocketAddr {
    fn from(value: SocketAddr) -> Self {
        value.inner
    }
}

uniffi::custom_type!(ImportPath, String);

#[derive(Debug, Clone)]
pub struct ImportPath {
    pub inner: InnerImportPath,
}

impl UniffiCustomTypeConverter for ImportPath {
    type Builtin = String;

    fn into_custom(val: Self::Builtin) -> uniffi::Result<Self> {
        let addr = parse_import_path(&val)?;
        Ok(Self { inner: addr })
    }

    fn from_custom(obj: Self) -> Self::Builtin {
        obj.inner.to_string()
    }
}

impl From<InnerImportPath> for ImportPath {
    fn from(value: InnerImportPath) -> Self {
        Self { inner: value }
    }
}

impl From<ImportPath> for InnerImportPath {
    fn from(value: ImportPath) -> Self {
        value.inner
    }
}
