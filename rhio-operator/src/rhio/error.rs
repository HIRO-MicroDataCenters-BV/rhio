use crate::api::service::RhioService;
use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    kube::{api::DynamicObject, core::error_boundary, runtime::reflector::ObjectRef},
    kvp::LabelError,
    logging::controller::ReconcilerError,
    role_utils::RoleGroupRef,
};
use strum::{EnumDiscriminants, IntoStaticStr};

#[derive(Snafu, Debug, EnumDiscriminants)]
#[strum_discriminants(derive(IntoStaticStr))]
#[allow(clippy::enum_variant_names)]
#[snafu(visibility(pub))]
pub enum Error {
    #[snafu(display("object has no name"))]
    ObjectHasNoName,

    #[snafu(display("invalid rhio service"))]
    InvalidRhioService {
        source: error_boundary::InvalidObject,
    },

    #[snafu(display("failed to create cluster resources"))]
    CreateClusterResources {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to delete orphaned resources"))]
    DeleteOrphans {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to update status"))]
    ApplyStatus {
        source: stackable_operator::client::Error,
    },

    #[snafu(display("failed to build Metadata"))]
    MetadataBuild {
        source: stackable_operator::builder::meta::Error,
    },

    #[snafu(display("failed to build Labels"))]
    LabelBuild {
        source: stackable_operator::kvp::LabelError,
    },

    #[snafu(display("invalid container name"))]
    InvalidContainerName {
        name: String,
        source: stackable_operator::builder::pod::container::Error,
    },

    #[snafu(display("add volume mount error"))]
    AddVolumeMount {
        source: stackable_operator::builder::pod::container::Error,
    },

    #[snafu(display("add volume error"))]
    AddVolume {
        source: stackable_operator::builder::pod::Error,
    },

    #[snafu(display("object {} is missing metadata to build owner reference", rhio_service))]
    ObjectMissingMetadataForOwnerRef {
        source: stackable_operator::builder::meta::Error,
        rhio_service: ObjectRef<RhioService>,
    },

    #[snafu(display("failed to build configmap that keeps rhio config.yaml"))]
    BuildConfigMap {
        source: crate::configuration::error::Error,
    },

    #[snafu(display("failed to apply Rhio ConfigMap"))]
    ApplyRhioConfig {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to serialize Rhio config"))]
    RhioConfigurationSerialization { source: serde_json::Error },

    #[snafu(display("failed to build label"))]
    BuildLabel { source: LabelError },

    #[snafu(display("failed to create RBAC service account"))]
    ApplyServiceAccount {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to create RBAC role binding"))]
    ApplyRoleBinding {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to build RBAC resources"))]
    BuildRbacResources {
        source: stackable_operator::commons::rbac::Error,
    },

    #[snafu(display("failed to apply global Service"))]
    ApplyRoleService {
        source: stackable_operator::cluster_resources::Error,
    },

    #[snafu(display("failed to calculate global service name"))]
    GlobalServiceNameNotFound,

    #[snafu(display("failed to build object  meta data"))]
    ObjectMeta {
        source: stackable_operator::builder::meta::Error,
    },

    #[snafu(display("object defines no server role"))]
    NoServerRole,

    #[snafu(display("failed to generate product config"))]
    GenerateProductConfig {
        source: stackable_operator::product_config_utils::Error,
    },

    #[snafu(display("invalid product config"))]
    InvalidProductConfig {
        source: stackable_operator::product_config_utils::Error,
    },

    #[snafu(display("failed to apply StatefulSet for {}", rolegroup))]
    ApplyRoleGroupStatefulSet {
        source: stackable_operator::cluster_resources::Error,
        rolegroup: RoleGroupRef<RhioService>,
    },

    #[snafu(display("object defines no namespace"))]
    ObjectHasNoNamespace,

    #[snafu(display("failed to get associated replicated message streams"))]
    GetReplicatedMessageStreams {
        source: stackable_operator::client::Error,
    },

    #[snafu(display("invalid annotation"))]
    InvalidAnnotation {
        source: stackable_operator::kvp::AnnotationError,
    },

    #[snafu(display("failed to configure graceful shutdown"))]
    GracefulShutdown {
        source: crate::operations::graceful_shutdown::Error,
    },
}

impl ReconcilerError for Error {
    fn category(&self) -> &'static str {
        ErrorDiscriminants::from(self).into()
    }

    fn secondary_object(&self) -> Option<ObjectRef<DynamicObject>> {
        match self {
            Error::ObjectHasNoName => None,
            _ => None, // TODO secondary object
        }
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
