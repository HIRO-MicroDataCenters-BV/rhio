#![allow(unused_imports, unused_variables)]
use crate::{api::service::{RhioService, RhioServiceStatus}, service_resource::build_rhio_statefulset};
use std::{
    borrow::Cow,
    collections::{BTreeMap, HashMap},
    sync::Arc,
};

use product_config::{
    types::PropertyNameKind,
    writer::{to_java_properties_string, PropertiesWriterError},
    ProductConfigManager,
};
use snafu::{OptionExt, ResultExt, Snafu};
use stackable_operator::{
    builder::{
        self,
        configmap::ConfigMapBuilder,
        meta::ObjectMetaBuilder,
        pod::{
            container::ContainerBuilder,
            resources::ResourceRequirementsBuilder,
            security::PodSecurityContextBuilder,
            volume::{ListenerOperatorVolumeSourceBuilder, ListenerReference, VolumeBuilder},
            PodBuilder,
        },
    },
    cluster_resources::{ClusterResourceApplyStrategy, ClusterResources},
    commons::{
        authentication::AuthenticationClass,
        listener::{Listener, ListenerPort, ListenerSpec},
        opa::OpaApiVersion,
        product_image_selection::ResolvedProductImage,
        rbac::build_rbac_resources,
    },
    k8s_openapi::{
        api::{
            apps::v1::{StatefulSet, StatefulSetSpec},
            core::v1::{
                ConfigMap, ConfigMapKeySelector, ConfigMapVolumeSource, ContainerPort, EnvVar,
                EnvVarSource, ExecAction, ObjectFieldSelector, PodSpec, Probe, Service,
                ServiceAccount, ServiceSpec, Volume,
            },
        },
        apimachinery::pkg::apis::meta::v1::LabelSelector,
        DeepMerge,
    },
    kube::{
        api::DynamicObject,
        core::{error_boundary, DeserializeGuard},
        runtime::{controller::Action, reflector::ObjectRef},
        Resource, ResourceExt,
    },
    kvp::{Label, Labels},
    logging::controller::ReconcilerError,
    memory::{BinaryMultiple, MemoryQuantity},
    product_config_utils::{transform_all_roles_to_config, validate_all_roles_and_groups_config},
    product_logging::{
        self,
        framework::LoggingError,
        spec::{
            ConfigMapLogConfig, ContainerLogConfig, ContainerLogConfigChoice,
            CustomContainerLogConfig,
        },
    },
    role_utils::{GenericRoleConfig, RoleGroupRef},
    status::condition::{
        compute_conditions, operations::ClusterOperationsConditionBuilder,
        statefulset::StatefulSetConditionBuilder,
    },
    time::Duration,
    utils::cluster_info::KubernetesClusterInfo,
};
use strum::{EnumDiscriminants, IntoStaticStr};

pub const APP_NAME: &str = "rhio";
pub const OPERATOR_NAME: &str = "rhio.hiro.io";
pub const RHIO_CONTROLLER_NAME: &str = "rhioservice";
pub const DOCKER_IMAGE_BASE_NAME: &str = "rhio";

pub struct Ctx {
    pub client: stackable_operator::client::Client,
    pub product_config: ProductConfigManager,
}

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
    MetadataBuildError {
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

}

impl ReconcilerError for Error {
    fn category(&self) -> &'static str {
        ErrorDiscriminants::from(self).into()
    }

    fn secondary_object(&self) -> Option<ObjectRef<DynamicObject>> {
        match self {
            Error::ObjectHasNoName => None,
            _ => None,
        }
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

pub async fn reconcile_rhio(
    rhio: Arc<DeserializeGuard<RhioService>>,
    ctx: Arc<Ctx>,
) -> Result<Action> {
    tracing::info!("Starting reconcile");

    let rhio_service = rhio
        .0
        .as_ref()
        .map_err(error_boundary::InvalidObject::clone)
        .context(InvalidRhioServiceSnafu)?;

    let resolved_product_image = rhio_service
        .spec
        .image
        .resolve(DOCKER_IMAGE_BASE_NAME, crate::built_info::PKG_VERSION);

    let client = &ctx.client;

    let cluster_resources = ClusterResources::new(
        APP_NAME,
        OPERATOR_NAME,
        RHIO_CONTROLLER_NAME,
        &rhio_service.object_ref(&()),
        ClusterResourceApplyStrategy::from(&rhio_service.spec.cluster_operation),
    )
    .context(CreateClusterResourcesSnafu)?;

    // let (rbac_sa, rbac_rolebinding) = build_rbac_resources(
    //     rhio_service,
    //     APP_NAME,
    //     cluster_resources
    //         .get_required_labels()
    //         .context(GetRequiredLabelsSnafu)?,
    //     )
    //     .context(BuildRbacResourcesSnafu)?;

    // let rbac_sa = cluster_resources
    //     .add(client, rbac_sa.clone())
    //     .await
    //     .context(ApplyServiceAccountSnafu)?;
    // cluster_resources
    //     .add(client, rbac_rolebinding)
    //     .await
    //     .context(ApplyRoleBindingSnafu)?;


    // let statefulset = build_rhio_statefulset(
    //     rhio_service,
    //     &resolved_product_image,
    //     &client.kubernetes_cluster_info,
    // )?;



    let status = RhioServiceStatus { conditions: vec![] };

    cluster_resources
        .delete_orphaned_resources(client)
        .await
        .context(DeleteOrphansSnafu)?;

    client
        .apply_patch_status(OPERATOR_NAME, rhio_service, &status)
        .await
        .context(ApplyStatusSnafu)?;

    Ok(Action::await_change())
}

pub fn error_policy(
    _obj: Arc<DeserializeGuard<RhioService>>,
    error: &Error,
    _ctx: Arc<Ctx>,
) -> Action {
    match error {
        Error::InvalidRhioService { .. } => Action::await_change(),
        _ => Action::requeue(*Duration::from_secs(5)),
    }
}
