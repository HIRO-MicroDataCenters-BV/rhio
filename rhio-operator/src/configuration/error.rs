use p2panda_core::IdentityError;
use snafu::Snafu;
use stackable_operator::{
    k8s_openapi::api::core::v1::Secret,
    kube::{api::DynamicObject, core::error_boundary, runtime::reflector::ObjectRef},
    logging::controller::ReconcilerError,
};
use std::string::FromUtf8Error;
use strum::{EnumDiscriminants, IntoStaticStr};

use crate::api::service::RhioService;

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
#[allow(clippy::enum_variant_names)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("Configuration resource is invalid"))]
    InvalidReplicatedResource {
        source: error_boundary::InvalidObject,
    },

    #[snafu(display("object defines no namespace"))]
    ObjectHasNoNamespace,

    #[snafu(display("object has no name"))]
    ObjectHasNoName,

    #[snafu(display("failed to get rhio services"))]
    GetRhioService {
        source: stackable_operator::client::Error,
    },

    #[snafu(display("failed to get rhio services"))]
    RhioIsAbsent,

    #[snafu(display("failed to update status"))]
    ApplyStatus {
        source: stackable_operator::client::Error,
    },

    #[snafu(display("failed to serialize Rhio config"))]
    RhioConfigurationSerialization { source: serde_json::Error },

    #[snafu(display("failed to deserialize secret"))]
    SecretDeserialization { source: serde_json::Error },

    #[snafu(display("failed to serialize secret"))]
    SecretSerialization { source: serde_json::Error },

    #[snafu(display("failed to serialize secret"))]
    YamlSerialization { source: serde_yaml::Error },

    #[snafu(display("Unable to make string from bytes"))]
    StringConversion { source: FromUtf8Error },

    #[snafu(display("secret has no string data"))]
    SecretHasNoData { secret: ObjectRef<Secret> },

    #[snafu(display("object {} is missing metadata to build owner reference", rhio))]
    ObjectMissingMetadataForOwnerRef {
        source: stackable_operator::builder::meta::Error,
        rhio: ObjectRef<RhioService>,
    },

    #[snafu(display("failed to build Metadata"))]
    MetadataBuild {
        source: stackable_operator::builder::meta::Error,
    },

    #[snafu(display("invalid NATS subject {subject}"))]
    InvalidNatsSubject {
        source: anyhow::Error,
        subject: String,
    },

    #[snafu(display("failed to build ConfigMap"))]
    BuildConfigMap {
        source: stackable_operator::builder::configmap::Error,
    },

    #[snafu(display("failed to parse public key {key}"))]
    PublicKeyParsing { source: IdentityError, key: String },

    #[snafu(display("failed to fetch secret {name}"))]
    GetSecret {
        source: stackable_operator::client::Error,
        name: String,
    },

    #[snafu(display("fail to write YAML to stdout"))]
    WriteToStdout { source: std::io::Error },

    #[snafu(display("multiple rhio services in the same namespace"))]
    MultipleServicesInTheSameNamespace { rhio: ObjectRef<RhioService> },

    #[snafu(display("Unable to resolve rhio service endpoint"))]
    GetRhioServiceEndpoint,
}

impl ReconcilerError for Error {
    fn category(&self) -> &'static str {
        ErrorDiscriminants::from(self).into()
    }

    fn secondary_object(&self) -> Option<ObjectRef<DynamicObject>> {
        match self {
            Error::ObjectHasNoName => None,
            Error::InvalidReplicatedResource { .. } => None,
            Error::ObjectHasNoNamespace => None,
            Error::GetRhioService { .. } => None,
            Error::RhioIsAbsent => None,
            Error::MultipleServicesInTheSameNamespace { rhio } => Some(rhio.clone().erase()),
            Error::ApplyStatus { .. } => None,
            Error::RhioConfigurationSerialization { .. } => None,
            Error::ObjectMissingMetadataForOwnerRef { rhio, .. } => Some(rhio.clone().erase()),
            Error::MetadataBuild { .. } => None,
            Error::InvalidNatsSubject { .. } => None,
            Error::BuildConfigMap { .. } => None,
            Error::PublicKeyParsing { .. } => None,
            Error::GetSecret { .. } => None,
            Error::SecretDeserialization { .. } => None,
            Error::SecretSerialization { .. } => None,
            Error::SecretHasNoData { secret } => Some(secret.clone().erase()),
            Error::WriteToStdout { .. } => None,
            Error::YamlSerialization { .. } => None,
            Error::GetRhioServiceEndpoint => None,
            Error::StringConversion { .. } => None,
        }
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
