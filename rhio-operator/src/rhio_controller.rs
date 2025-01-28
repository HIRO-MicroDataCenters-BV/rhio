use crate::{
    api::{
        message_stream::ReplicatedMessageStream,
        message_stream_subscription::ReplicatedMessageStreamSubscription,
        object_store::ReplicatedObjectStore,
        object_store_subscription::ReplicatedObjectStoreSubscription,
        service::{RhioService, RhioServiceStatus},
    },
    configuration::RhioConfigurationResources,
    service_resource::{build_rhio_statefulset, build_server_role_service},
    status::fetch_status,
};
use futures::StreamExt;
use product_config::ProductConfigManager;
use rhio_http_api::status::HealthStatus;
use snafu::{ResultExt, Snafu};
use stackable_operator::kube::ResourceExt;
use stackable_operator::{client::Client, kube::runtime::Controller, namespace::WatchNamespace};
use stackable_operator::{
    cluster_resources::{ClusterResourceApplyStrategy, ClusterResources},
    commons::rbac::build_rbac_resources,
    kube::{
        api::DynamicObject,
        core::{error_boundary, DeserializeGuard},
        runtime::{controller::Action, reflector::ObjectRef},
        Resource,
    },
    kvp::LabelError,
    logging::controller::ReconcilerError,
    role_utils::RoleGroupRef,
    status::condition::{
        compute_conditions, operations::ClusterOperationsConditionBuilder,
        statefulset::StatefulSetConditionBuilder,
    },
};
use stackable_operator::{
    k8s_openapi::api::{
        apps::v1::StatefulSet,
        core::v1::{ConfigMap, Service, ServiceAccount},
        rbac::v1::RoleBinding,
    },
    kube::runtime::watcher,
    logging::controller::report_controller_reconciled,
};
use std::{sync::Arc, time::Duration};
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
    let mut requeue_duration = Duration::from_secs(5 * 60);

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
    let rolegroup = rhio_service.server_rolegroup_ref();

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

    cluster_resources
        .add(
            client,
            build_server_role_service(rhio_service, &resolved_product_image, &rolegroup)?,
        )
        .await
        .context(ApplyRoleServiceSnafu)?;

    let (rhio_configmap, config_map_hash) =
        RhioConfigurationResources::load(client.clone(), rhio_service)
            .await?
            .build_rhio_configmap(&resolved_product_image)?;

    let mut ss_cond_builder = StatefulSetConditionBuilder::default();

    cluster_resources
        .add(client, rhio_configmap)
        .await
        .context(ApplyRhioConfigSnafu)?;

    let rhio_statefulset = build_rhio_statefulset(
        rhio_service,
        &resolved_product_image,
        &rolegroup,
        &rbac_sa,
        config_map_hash,
    )?;

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

    let maybe_health_status = fetch_status(rhio_service).await;
    let health_status = match maybe_health_status {
        Ok(health_status) => health_status,
        Err(_e) => {
            requeue_duration = Duration::from_secs(5);
            HealthStatus::default()
        }
    };

    let status = RhioServiceStatus {
        conditions: compute_conditions(
            rhio_service,
            &[&ss_cond_builder, &cluster_operation_cond_builder],
        ),
        status: health_status,
    };

    cluster_resources
        .delete_orphaned_resources(client)
        .await
        .context(DeleteOrphansSnafu)?;

    client
        .apply_patch_status(OPERATOR_NAME, rhio_service, &status)
        .await
        .context(ApplyStatusSnafu)?;

    Ok(Action::requeue(requeue_duration))
}

pub fn error_policy(
    _obj: Arc<DeserializeGuard<RhioService>>,
    error: &Error,
    _ctx: Arc<Ctx>,
) -> Action {
    match error {
        Error::InvalidRhioService { .. } => Action::await_change(),
        _ => Action::requeue(Duration::from_secs(5)),
    }
}

pub async fn create_rhio_controller(
    client: &Client,
    product_config: ProductConfigManager,
    namespace: WatchNamespace,
) {
    let rhio_controller = Controller::new(
        namespace.get_api::<DeserializeGuard<RhioService>>(client),
        watcher::Config::default(),
    );

    let rhio_store_streams = rhio_controller.store();
    let rhio_store_stores = rhio_controller.store();
    let rhio_store_stream_subs_store = rhio_controller.store();
    let rhio_store_bucket_subs_store = rhio_controller.store();

    let rhio_controller = rhio_controller
        .watches(
            namespace.get_api::<DeserializeGuard<ReplicatedMessageStream>>(client),
            watcher::Config::default(),
            move |stream| {
                rhio_store_streams
                    .state()
                    .into_iter()
                    .filter(move |rhio| {
                        let Ok(rhio) = &rhio.0 else {
                            return false;
                        };
                        let Ok(stream) = &stream.0 else {
                            return false;
                        };
                        stream.namespace() == rhio.namespace()
                    })
                    .map(|rhio| ObjectRef::from_obj(&*rhio))
            },
        )
        .watches(
            namespace.get_api::<DeserializeGuard<ReplicatedObjectStore>>(client),
            watcher::Config::default(),
            move |object_store| {
                rhio_store_stores
                    .state()
                    .into_iter()
                    .filter(move |rhio| {
                        let Ok(rhio) = &rhio.0 else {
                            return false;
                        };
                        let Ok(object_store) = &object_store.0 else {
                            return false;
                        };
                        object_store.namespace() == rhio.namespace()
                    })
                    .map(|rhio| ObjectRef::from_obj(&*rhio))
            },
        )
        .watches(
            namespace.get_api::<DeserializeGuard<ReplicatedMessageStreamSubscription>>(client),
            watcher::Config::default(),
            move |subs| {
                rhio_store_stream_subs_store
                    .state()
                    .into_iter()
                    .filter(move |rhio| {
                        let Ok(rhio) = &rhio.0 else {
                            return false;
                        };
                        let Ok(subs) = &subs.0 else {
                            return false;
                        };
                        subs.namespace() == rhio.namespace()
                    })
                    .map(|rhio| ObjectRef::from_obj(&*rhio))
            },
        )
        .watches(
            namespace.get_api::<DeserializeGuard<ReplicatedObjectStoreSubscription>>(client),
            watcher::Config::default(),
            move |subs| {
                rhio_store_bucket_subs_store
                    .state()
                    .into_iter()
                    .filter(move |rhio| {
                        let Ok(rhio) = &rhio.0 else {
                            return false;
                        };
                        let Ok(subs) = &subs.0 else {
                            return false;
                        };
                        subs.namespace() == rhio.namespace()
                    })
                    .map(|rhio| ObjectRef::from_obj(&*rhio))
            },
        )
        .owns(
            namespace.get_api::<StatefulSet>(client),
            watcher::Config::default(),
        )
        .owns(
            namespace.get_api::<Service>(client),
            watcher::Config::default(),
        )
        .owns(
            namespace.get_api::<ConfigMap>(client),
            watcher::Config::default(),
        )
        .owns(
            namespace.get_api::<ServiceAccount>(client),
            watcher::Config::default(),
        )
        .owns(
            namespace.get_api::<RoleBinding>(client),
            watcher::Config::default(),
        )
        .shutdown_on_signal()
        .run(
            reconcile_rhio,
            error_policy,
            Arc::new(Ctx {
                client: client.clone(),
                product_config,
            }),
        )
        .map(|res| {
            report_controller_reconciled(
                client,
                &format!("{RHIO_CONTROLLER_NAME}.{OPERATOR_NAME}"),
                &res,
            );
        })
        .collect::<()>();
    rhio_controller.await
}

#[cfg(test)]
mod tests {

    // use super::*;
}
