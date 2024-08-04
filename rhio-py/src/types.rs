use std::path::PathBuf;
use std::str::FromStr;

use p2panda_core::Hash as InnerHash;

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
        Ok(Hash { inner: hash })
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
