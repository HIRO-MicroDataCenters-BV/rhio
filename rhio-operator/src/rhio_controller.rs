#![allow(unused_imports, unused_variables)]
use crate::{
    api::{
        message_stream::ReplicatedMessageStream,
        message_stream_subscription::ReplicatedMessageStreamSubscription,
        object_store::ReplicatedObjectStore,
        object_store_subscription::ReplicatedObjectStoreSubscription,
        role::RhioRole,
        service::{HasServiceRef, RhioService, RhioServiceStatus},
    },
    service_resource::{build_recommended_labels, build_rhio_configmap, build_rhio_statefulset},
};
use std::{
    borrow::Cow,
    collections::{BTreeMap, HashMap},
    str::FromStr,
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
                ServiceAccount, ServicePort, ServiceSpec, Volume,
            },
        },
        apimachinery::pkg::apis::meta::v1::LabelSelector,
        DeepMerge,
    },
    kube::{
        api::{DynamicObject, ListParams},
        core::{error_boundary, DeserializeGuard},
        runtime::{controller::Action, reflector::ObjectRef},
        Resource, ResourceExt,
    },
    kvp::{Label, LabelError, Labels},
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

    #[snafu(display("object {} is missing metadata to build owner reference", rhio_service))]
    ObjectMissingMetadataForOwnerRef {
        source: stackable_operator::builder::meta::Error,
        rhio_service: ObjectRef<RhioService>,
    },

    #[snafu(display("failed to build ConfigMap"))]
    BuildConfigMap {
        source: stackable_operator::builder::configmap::Error,
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

    #[snafu(display("invalid NATS subject {subject}"))]
    InvalidNatsSubject {
        source: anyhow::Error,
        subject: String,
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

    let rhio_service: &RhioService = rhio
        .0
        .as_ref()
        .map_err(error_boundary::InvalidObject::clone)
        .context(InvalidRhioServiceSnafu)?;

    let resolved_product_image = rhio_service
        .spec
        .image
        .resolve(DOCKER_IMAGE_BASE_NAME, crate::built_info::PKG_VERSION);

    let client = &ctx.client;

    let mut cluster_resources = ClusterResources::new(
        APP_NAME,
        OPERATOR_NAME,
        RHIO_CONTROLLER_NAME,
        &rhio_service.object_ref(&()),
        ClusterResourceApplyStrategy::from(&rhio_service.spec.cluster_operation),
    )
    .context(CreateClusterResourcesSnafu)?;

    let (rbac_sa, rbac_rolebinding) = build_rbac_resources(
        rhio_service,
        APP_NAME,
        cluster_resources
            .get_required_labels()
            .context(BuildLabelSnafu)?,
    )
    .context(BuildRbacResourcesSnafu)?;

    cluster_resources
        .add(client, rbac_sa.clone())
        .await
        .context(ApplyServiceAccountSnafu)?;

    cluster_resources
        .add(client, rbac_rolebinding)
        .await
        .context(ApplyRoleBindingSnafu)?;

    let server_role_service = cluster_resources
        .add(
            client,
            build_server_role_service(rhio_service, &resolved_product_image)?,
        )
        .await
        .context(ApplyRoleServiceSnafu)?;

    let streams = client
        .list::<ReplicatedMessageStream>(
            rhio_service
                .meta()
                .namespace
                .as_deref()
                .context(ObjectHasNoNamespaceSnafu)?,
            &ListParams::default(),
        )
        .await
        .context(GetReplicatedMessageStreamsSnafu)?
        .into_iter()
        .filter(|stream| stream.service_ref() == rhio_service.service_ref())
        .collect::<Vec<ReplicatedMessageStream>>();

    let stores = client
        .list::<ReplicatedObjectStore>(
            rhio_service
                .meta()
                .namespace
                .as_deref()
                .context(ObjectHasNoNamespaceSnafu)?,
            &ListParams::default(),
        )
        .await
        .context(GetReplicatedMessageStreamsSnafu)?
        .into_iter()
        .filter(|stream| stream.service_ref() == rhio_service.service_ref())
        .collect::<Vec<ReplicatedObjectStore>>();

    let stream_subscriptions = client
        .list::<ReplicatedMessageStreamSubscription>(
            rhio_service
                .meta()
                .namespace
                .as_deref()
                .context(ObjectHasNoNamespaceSnafu)?,
            &ListParams::default(),
        )
        .await
        .context(GetReplicatedMessageStreamsSnafu)?
        .into_iter()
        .filter(|stream| stream.service_ref() == rhio_service.service_ref())
        .collect::<Vec<ReplicatedMessageStreamSubscription>>();

    let store_subscriptions = client
        .list::<ReplicatedObjectStoreSubscription>(
            rhio_service
                .meta()
                .namespace
                .as_deref()
                .context(ObjectHasNoNamespaceSnafu)?,
            &ListParams::default(),
        )
        .await
        .context(GetReplicatedMessageStreamsSnafu)?
        .into_iter()
        .filter(|stream| stream.service_ref() == rhio_service.service_ref())
        .collect::<Vec<ReplicatedObjectStoreSubscription>>();

    let rolegroup = rhio_service.server_rolegroup_ref(RhioRole::Server.to_string());

    let rhio_configmap = build_rhio_configmap(
        rhio_service,
        streams,
        stream_subscriptions,
        stores,
        store_subscriptions,
        rhio_service,
        &resolved_product_image,
        &rolegroup,
    )?;

    let mut ss_cond_builder = StatefulSetConditionBuilder::default();

    cluster_resources
        .add(client, rhio_configmap)
        .await
        .context(ApplyRhioConfigSnafu)?;

    let rhio_statefulset =
        build_rhio_statefulset(rhio_service, &resolved_product_image, &rolegroup, &rbac_sa)?;

    ss_cond_builder.add(
        cluster_resources
            .add(client, rhio_statefulset)
            .await
            .with_context(|_| ApplyRoleGroupStatefulSetSnafu {
                rolegroup: rolegroup.clone(),
            })?,
    );

    let cluster_operation_cond_builder =
        ClusterOperationsConditionBuilder::new(&rhio_service.spec.cluster_operation);

    let status = RhioServiceStatus {
        conditions: compute_conditions(
            rhio_service,
            &[&ss_cond_builder, &cluster_operation_cond_builder],
        ),
    };

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

/// The server-role service is the primary endpoint that should be used by clients that do not perform internal load balancing,
/// including targets outside of the cluster.
///
pub fn build_server_role_service(
    rhio: &RhioService,
    resolved_product_image: &ResolvedProductImage,
) -> Result<Service> {
    let role_name = RhioRole::Server.to_string();
    let role_svc_name = rhio
        .server_role_service_name()
        .context(GlobalServiceNameNotFoundSnafu)?;

    let metadata = ObjectMetaBuilder::new()
        .name_and_namespace(rhio)
        .name(&role_svc_name)
        .ownerreference_from_resource(rhio, None, Some(true))
        .context(ObjectMissingMetadataForOwnerRefSnafu { rhio_service: rhio })?
        .with_recommended_labels(build_recommended_labels(
            rhio,
            RHIO_CONTROLLER_NAME,
            &resolved_product_image.app_version_label,
            &role_name,
            "global",
        ))
        .context(ObjectMetaSnafu)?
        .build();

    let service_selector_labels =
        Labels::role_selector(rhio, APP_NAME, &role_name).context(BuildLabelSnafu)?;

    let service_spec = ServiceSpec {
        ports: Some(vec![ServicePort {
            name: Some("rhio".to_string()),
            port: 9102,
            protocol: Some("UDP".to_string()),
            ..ServicePort::default()
        }]),
        selector: Some(service_selector_labels.into()),
        type_: Some(rhio.spec.cluster_config.listener_class.k8s_service_type()),
        ..ServiceSpec::default()
    };

    Ok(Service {
        metadata,
        spec: Some(service_spec),
        status: None,
    })
}

#[cfg(test)]
mod tests {

    use super::*;
}
