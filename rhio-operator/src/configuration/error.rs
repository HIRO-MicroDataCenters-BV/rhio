use p2panda_core::IdentityError;
use snafu::Snafu;
use stackable_operator::{
    kube::{api::DynamicObject, core::error_boundary, runtime::reflector::ObjectRef},
    logging::controller::ReconcilerError,
};
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

    #[snafu(display("Rhio Service has no status"))]
    RhioServiceHasNoStatus,

    #[snafu(display("Rhio Service has no status"))]
    RhioServiceHasNoStatusForStream,

    #[snafu(display("failed to update status"))]
    ApplyStatus {
        source: stackable_operator::client::Error,
    },

    #[snafu(display("failed to serialize Rhio config"))]
    RhioConfigurationSerialization { source: serde_json::Error },

    #[snafu(display("object {} is missing metadata to build owner reference", rhio_service))]
    ObjectMissingMetadataForOwnerRef {
        source: stackable_operator::builder::meta::Error,
        rhio_service: ObjectRef<RhioService>,
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
}

impl ReconcilerError for Error {
    fn category(&self) -> &'static str {
        ErrorDiscriminants::from(self).into()
    }
    
    // TODO add secondary objects
    fn secondary_object(&self) -> Option<ObjectRef<DynamicObject>> {
        match self {
            Error::ObjectHasNoName => None,
            Error::InvalidReplicatedResource { .. } => None,
            Error::ObjectHasNoNamespace => None,
            Error::GetRhioService { .. } => None,
            Error::RhioIsAbsent => None,
            Error::RhioServiceHasNoStatus => None,
            Error::RhioServiceHasNoStatusForStream => None,
            Error::ApplyStatus { .. } => None,
            Error::RhioConfigurationSerialization { .. } => None,
            Error::ObjectMissingMetadataForOwnerRef { .. } => None,
            Error::MetadataBuild { .. } => None,
            Error::InvalidNatsSubject { .. } => None,
            Error::BuildConfigMap { .. } => None,
            Error::PublicKeyParsing { .. } => None,
        }
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
