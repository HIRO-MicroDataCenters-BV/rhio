use crate::configuration::error::GetSecretSnafu;
use crate::configuration::error::Result;
use serde_json::Value;
use snafu::{OptionExt, ResultExt};
use stackable_operator::builder::meta::ObjectMetaBuilder;
use stackable_operator::client::Client;
use stackable_operator::kube::runtime::reflector::Lookup;
use stackable_operator::kube::runtime::reflector::ObjectRef;
use std::collections::BTreeMap;
use std::io::Write;

use super::error::SecretHasNoStringDataSnafu;
use super::error::WriteToStdoutSnafu;
use super::error::YamlSerializationSnafu;
use super::error::{
    ObjectHasNoNameSnafu, ObjectHasNoNamespaceSnafu, SecretDeserializationSnafu,
    SecretSerializationSnafu,
};

/// A struct representing a Kubernetes Secret with generic data type `T`.
///
/// This struct provides methods to create, fetch, serialize, and deserialize Kubernetes Secrets
/// with the specified data type `T`. The data type `T` must implement the `serde::Serialize` and
/// `serde::Deserialize` traits.
///
/// # Type Parameters
/// - `T`: The type of the data stored in the secret. It must implement `serde::Serialize` and
///   `for<'de> serde::Deserialize<'de>`.
///
/// # Fields
/// - `value`: The value of the secret of type `T`.
/// - `name`: The name of the secret.
/// - `namespace`: The namespace of the secret.
///
/// # Methods
/// - `new(name: String, namespace: String, value: T) -> Secret<T>`: Creates a new `Secret` instance.
/// - `value(&self) -> &T`: Returns a reference to the value of the secret.
/// - `fetch(client: &Client, name: &str, namespace: &str) -> Result<Secret<T>>`: Fetches a secret
///   from the Kubernetes cluster using the provided client, name, and namespace.
/// - `to_secret(&self) -> Result<stackable_operator::k8s_openapi::api::core::v1::Secret>`: Converts
///   the `Secret` instance to a Kubernetes Secret object.
/// - `print_yaml(&self) -> Result<()>`: Prints the secret as a YAML string to stdout.
/// - `from(k8s_secret: stackable_operator::k8s_openapi::api::core::v1::Secret) -> Result<Secret<T>>`:
///   Creates a `Secret` instance from a Kubernetes Secret object.
///
pub struct Secret<T>
where
    T: serde::Serialize + for<'de> serde::Deserialize<'de>,
{
    value: T,
    name: String,
    namespace: String,
}

impl<T> Secret<T>
where
    T: serde::Serialize + for<'de> serde::Deserialize<'de>,
{
    pub fn new(name: String, namespace: String, value: T) -> Secret<T> {
        Secret {
            value,
            name,
            namespace,
        }
    }

    pub fn value(&self) -> &T {
        &self.value
    }

    pub async fn fetch(client: &Client, name: &str, namespace: &str) -> Result<Secret<T>> {
        let secret = client
            .get::<stackable_operator::k8s_openapi::api::core::v1::Secret>(name, namespace)
            .await
            .context(GetSecretSnafu { name })?;
        Secret::from(secret)
    }

    pub fn to_secret(&self) -> Result<stackable_operator::k8s_openapi::api::core::v1::Secret> {
        let data = Secret::to_data(&self.value)?;
        Ok(self.build_k8s_secret(data))
    }

    pub fn print_yaml(&self) -> Result<()> {
        let secret = self.to_secret()?;

        let serialized_secret = serde_yaml::to_string(&secret).context(YamlSerializationSnafu)?;

        let mut writer = std::io::stdout();
        writer
            .write_all(serialized_secret.as_bytes())
            .context(WriteToStdoutSnafu)?;

        Ok(())
    }

    fn to_data(value: &T) -> Result<BTreeMap<String, String>> {
        let json_value = serde_json::to_value(value).context(SecretSerializationSnafu)?;

        let result = match json_value {
            Value::Object(map) => map
                .into_iter()
                .flat_map(|(k, v)| match v {
                    Value::String(s) => Some((k, s.to_string())),
                    Value::Bool(b) => Some((k, b.to_string())),
                    Value::Number(n) => Some((k, n.to_string())),
                    _ => None,
                })
                .collect::<BTreeMap<String, String>>(),
            _ => BTreeMap::new(),
        };
        Ok(result)
    }

    fn build_k8s_secret(
        &self,
        data: BTreeMap<String, String>,
    ) -> stackable_operator::k8s_openapi::api::core::v1::Secret {
        stackable_operator::k8s_openapi::api::core::v1::Secret {
            immutable: Some(true),
            metadata: ObjectMetaBuilder::new()
                .name(&self.name)
                .namespace_opt(Some(self.namespace.clone()))
                .build(),
            string_data: Some(data),
            ..stackable_operator::k8s_openapi::api::core::v1::Secret::default()
        }
    }

    pub fn from(
        k8s_secret: stackable_operator::k8s_openapi::api::core::v1::Secret,
    ) -> Result<Secret<T>> {
        let name = k8s_secret.name().context(ObjectHasNoNameSnafu)?.to_string();
        let namespace = k8s_secret
            .namespace()
            .context(ObjectHasNoNamespaceSnafu)?
            .to_string();
        let obj_ref = ObjectRef::from_obj(&k8s_secret);
        let data = k8s_secret
            .string_data
            .context(SecretHasNoStringDataSnafu { secret: obj_ref })?;
        let value = Secret::from_data(data)?;
        Ok(Secret {
            value,
            name,
            namespace,
        })
    }

    fn from_data(data: BTreeMap<String, String>) -> Result<T> {
        let value_data = data
            .into_iter()
            .map(|(k, v)| (k, Value::String(v)))
            .collect::<serde_json::Map<String, Value>>();
        let json_value = Value::Object(value_data);
        serde_json::from_value(json_value).context(SecretDeserializationSnafu)
    }
}

#[cfg(test)]
mod tests {
    use rhio_config::configuration::NatsCredentials;
    use s3::creds::Credentials;

    use crate::rhio::private_key::PrivateKey;

    use super::*;

    #[test]
    fn test_nats_serialization() {
        serde_object(NatsCredentials {
            nkey: None,
            username: None,
            password: None,
            token: None,
        });

        serde_object(NatsCredentials {
            nkey: Some("nkey".into()),
            username: Some("user".into()),
            password: Some("password".into()),
            token: None,
        });
    }

    #[test]
    fn test_s3_serialization() {
        let value = Credentials {
            access_key: Some("access_key".into()),
            secret_key: Some("secret_key".into()),
            security_token: None,
            session_token: None,
            expiration: None,
        };

        let serialized = serialize(&value);
        let deserialized = deserialize::<Credentials>(serialized);
        assert_eq!(deserialized.access_key, value.access_key);
        assert_eq!(deserialized.secret_key, value.secret_key);
        assert_eq!(deserialized.security_token, value.security_token);
        assert_eq!(deserialized.session_token, value.session_token);
        assert_eq!(deserialized.expiration, value.expiration);
    }

    #[test]
    fn test_private_key_serialization() {
        serde_object(PrivateKey {
            secret_key: "key".into(),
            public_key: "pkey".into(),
        });
    }

    fn serde_object<T>(value: T)
    where
        T: serde::Serialize
            + for<'de> serde::Deserialize<'de>
            + Clone
            + PartialEq
            + std::fmt::Debug,
    {
        let serialized = serialize(&value);
        let deserialized_value = deserialize::<T>(serialized);

        assert_eq!(deserialized_value, value);
    }

    fn serialize<T>(value: &T) -> String
    where
        T: serde::Serialize
            + for<'de> serde::Deserialize<'de>
            + Clone
            + PartialEq
            + std::fmt::Debug,
    {
        let secret = Secret::new("name".into(), "ns".into(), value.clone());
        let sec = secret
            .to_secret()
            .expect("unable to serialize object into k8s secret");
        serde_yaml::to_string(&sec).expect("unable to serialize k8s secret")
    }

    fn deserialize<T>(serialized: String) -> T
    where
        T: serde::Serialize
            + for<'de> serde::Deserialize<'de>
            + Clone
            + PartialEq
            + std::fmt::Debug,
    {
        let k8s_secret: stackable_operator::k8s_openapi::api::core::v1::Secret =
            serde_yaml::from_str(&serialized).expect("unable to deserialize k8s secret");

        Secret::<T>::from(k8s_secret)
            .expect("unable to deserialize object from k8s secret")
            .value()
            .to_owned()
    }
}
