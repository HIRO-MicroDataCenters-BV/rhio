use std::fmt;
use std::str::FromStr;

use anyhow::bail;
use async_nats::subject::Subject as NatsSubject;
use serde::{Deserialize, Serialize};

const DELIMITER_TOKEN: &str = ".";
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
        Ok(Self::new(value))
    }
}

impl TryFrom<NatsSubject> for Subject {
    type Error = anyhow::Error;

    fn try_from(value: NatsSubject) -> Result<Self, Self::Error> {
        Self::from_str(&value)
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

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::Subject;

    #[test]
    fn subject_filter_matching() {
        let assert_filter = |a: String, b: String, expected: bool| {
            assert!(
                Subject::from_str(&a)
                    .unwrap()
                    .is_matching(&Subject::from_str(&b).unwrap())
                    == expected,
                "{a} does not match {b}"
            );
        };

        assert_filter("a.*".to_string(), "a.b".to_string(), true);
        assert_filter("a.*".to_string(), "a.b.c".to_string(), false);
        assert_filter("a.*.c".to_string(), "a.b.c".to_string(), true);
        assert_filter("*.*".to_string(), "a.b".to_string(), true);
        assert_filter("*.c".to_string(), "a.b.c".to_string(), false);
        assert_filter("*.*".to_string(), "a.b.c".to_string(), false);
        assert_filter("*.*.*".to_string(), "a.b.c".to_string(), true);
        assert_filter("a.b".to_string(), "a.b.c".to_string(), false);
        assert_filter("a.b.*".to_string(), "a.*.c".to_string(), true);
    }
}
