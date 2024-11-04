use std::fmt;
use std::str::FromStr;

use anyhow::{anyhow, bail};
use p2panda_core::PublicKey;
use serde::{Deserialize, Serialize};

const DELIMITER_TOKEN: &str = ".";
const KEY_PREFIX_TOKEN: &str = "/";
const WILDCARD_TOKEN: &str = "*";

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct RhioSubject(PublicKey, Vec<String>);

impl RhioSubject {
    pub fn new(public_key: PublicKey, subject: &str) -> Self {
        Self(
            public_key,
            subject
                .split(DELIMITER_TOKEN)
                .map(|value| value.to_string())
                .collect(),
        )
    }

    pub fn public_key(&self) -> PublicKey {
        self.0
    }

    pub fn subject(&self) -> String {
        self.1.join(DELIMITER_TOKEN)
    }

    pub fn is_matching(&self, other_subject: &RhioSubject) -> bool {
        if self.0 != other_subject.0 {
            return false;
        }

        if self.1.len() != other_subject.1.len() {
            return false;
        }

        for (index, token) in self.1.iter().enumerate() {
            let Some(other_token) = other_subject.1.get(index) else {
                unreachable!("compared subjects should be of same length");
            };

            if token == other_token {
                continue;
            }

            if token == WILDCARD_TOKEN {
                continue;
            }

            if other_token == WILDCARD_TOKEN {
                continue;
            }

            return false;
        }

        return true;
    }
}

impl fmt::Display for RhioSubject {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}{}{}", self.0, KEY_PREFIX_TOKEN, self.subject())
    }
}

impl FromStr for RhioSubject {
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

        let nats_subject = parts[1].to_string();
        if nats_subject.is_empty() {
            bail!("can't have empty nats subject string");
        }

        Ok(Self::new(public_key, &nats_subject))
    }
}

impl Serialize for RhioSubject {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for RhioSubject {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value: String = String::deserialize(deserializer)?.into();
        Ok(Self::from_str(&value).map_err(|err| serde::de::Error::custom(err.to_string()))?)
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use p2panda_core::PrivateKey;

    use super::RhioSubject;

    #[test]
    fn serde() {
        let private_key = PrivateKey::new();
        let subject = RhioSubject::new(private_key.public_key(), "bla.*.blubb");

        let mut bytes = Vec::new();
        ciborium::into_writer(&subject, &mut bytes).unwrap();
        let subject_again: RhioSubject = ciborium::from_reader(&bytes[..]).unwrap();

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
        assert!(RhioSubject::from_str("invalid_public_key/boo.*").is_err());
        assert!(RhioSubject::from_str(&format!("{public_key}/boo.*/too_much")).is_err());
        assert!(RhioSubject::from_str(&public_key.to_string()).is_err());
    }

    #[test]
    fn subject_filter_matching() {
        let private_key = PrivateKey::new();
        let public_key = private_key.public_key();

        let assert_filter = |a: String, b: String, expected: bool| {
            assert!(
                RhioSubject::from_str(&a)
                    .unwrap()
                    .is_matching(&RhioSubject::from_str(&b).unwrap())
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
