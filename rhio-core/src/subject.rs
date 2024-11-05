use std::fmt;
use std::str::FromStr;

use anyhow::{anyhow, bail};
use p2panda_core::PublicKey;
use serde::{Deserialize, Serialize};

const DELIMITER_TOKEN: &str = ".";
const KEY_PREFIX_TOKEN: &str = "/";
const WILDCARD_TOKEN: &str = "*";

type Token = String;

/// Filterable NATS subject.
///
/// For example: `hello.*.world`
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Subject(Vec<Token>);

impl Subject {
    fn new(value: &str) -> Self {
        Self(
            value
                .split(DELIMITER_TOKEN)
                .map(|value| value.to_string())
                .collect(),
        )
    }

    /// Returns true if a given, second subject is a sub-set of this one.
    ///
    /// The matching rules are quite simple in NATS and do not resemble typical "globbing" rules,
    /// on top they are not hierarchical.
    ///
    /// For example `hello.*` does not match `hello.*.world` as both subjects need to have the same
    /// lenght. `hello.*.*` would match though.
    pub fn is_matching(&self, other_subject: &Subject) -> bool {
        if self.0.len() != other_subject.0.len() {
            return false;
        }

        for (index, token) in self.0.iter().enumerate() {
            let Some(other_token) = other_subject.0.get(index) else {
                unreachable!("compared subjects should be of same length");
            };

            if token == other_token || token == WILDCARD_TOKEN || other_token == WILDCARD_TOKEN {
                continue;
            }

            return false;
        }

        true
    }
}

impl fmt::Display for Subject {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0.join(DELIMITER_TOKEN))
    }
}

impl FromStr for Subject {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        if value.is_empty() {
            bail!("can't have empty nats subject string");
        }
        Ok(Self::new(&value))
    }
}

impl Serialize for Subject {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for Subject {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value: String = String::deserialize(deserializer)?;
        Self::from_str(&value).map_err(|err| serde::de::Error::custom(err.to_string()))
    }
}

/// Special subject for the "rhio" application which holds a (filterable) NATS subject next to a
/// peer's Ed25519 public key.
///
/// This makes the NATS subject "scoped" to this particular identity.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ScopedSubject(PublicKey, Subject);

impl ScopedSubject {
    pub fn new(public_key: PublicKey, subject: &str) -> Self {
        Self(public_key, Subject::new(subject))
    }

    pub fn public_key(&self) -> PublicKey {
        self.0
    }

    pub fn subject(&self) -> Subject {
        self.1.clone()
    }

    pub fn is_matching(&self, other_subject: &ScopedSubject) -> bool {
        if self.0 != other_subject.0 {
            return false;
        }

        self.1.is_matching(&other_subject.1)
    }
}

impl fmt::Display for ScopedSubject {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}{}{}", self.0, KEY_PREFIX_TOKEN, self.subject())
    }
}

impl FromStr for ScopedSubject {
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

        Ok(Self(public_key, Subject::from_str(parts[1])?))
    }
}

impl Serialize for ScopedSubject {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for ScopedSubject {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value: String = String::deserialize(deserializer)?;
        Self::from_str(&value).map_err(|err| serde::de::Error::custom(err.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use p2panda_core::PrivateKey;

    use super::ScopedSubject;

    #[test]
    fn serde() {
        let private_key = PrivateKey::new();
        let subject = ScopedSubject::new(private_key.public_key(), "bla.*.blubb");

        let mut bytes = Vec::new();
        ciborium::into_writer(&subject, &mut bytes).unwrap();
        let subject_again: ScopedSubject = ciborium::from_reader(&bytes[..]).unwrap();

        assert_eq!(subject, subject_again);
        assert_eq!(
            subject.to_string(),
            format!("{}/bla.*.blubb", private_key.public_key())
        );
    }

    #[test]
    fn invalid_values() {
        let private_key = PrivateKey::new();
        let public_key = private_key.public_key();
        assert!(ScopedSubject::from_str("invalid_public_key/boo.*").is_err());
        assert!(ScopedSubject::from_str(&format!("{public_key}/boo.*/too_much")).is_err());
        assert!(ScopedSubject::from_str(&public_key.to_string()).is_err());
    }

    #[test]
    fn subject_filter_matching() {
        let private_key = PrivateKey::new();
        let public_key = private_key.public_key();

        let assert_filter = |a: String, b: String, expected: bool| {
            assert!(
                ScopedSubject::from_str(&a)
                    .unwrap()
                    .is_matching(&ScopedSubject::from_str(&b).unwrap())
                    == expected,
                "{a} does not match {b}"
            );
        };

        assert_filter(
            format!("{public_key}/a.*"),
            format!("{public_key}/a.b"),
            true,
        );
        assert_filter(
            format!("{public_key}/a.*"),
            format!("{public_key}/a.b.c"),
            false,
        );
        assert_filter(
            format!("{public_key}/a.*.c"),
            format!("{public_key}/a.b.c"),
            true,
        );
        assert_filter(
            format!("{public_key}/*.*"),
            format!("{public_key}/a.b"),
            true,
        );
        assert_filter(
            format!("{public_key}/*.c"),
            format!("{public_key}/a.b.c"),
            false,
        );
        assert_filter(
            format!("{public_key}/*.*"),
            format!("{public_key}/a.b.c"),
            false,
        );
        assert_filter(
            format!("{public_key}/*.*.*"),
            format!("{public_key}/a.b.c"),
            true,
        );
        assert_filter(
            format!("{public_key}/a.b"),
            format!("{public_key}/a.b.c"),
            false,
        );
        assert_filter(
            format!("{public_key}/a.b.*"),
            format!("{public_key}/a.*.c"),
            true,
        );
    }
}
