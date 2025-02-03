use crate::rhio::error::BuildLabelSnafu;
use crate::{
    api::{
        message_stream::ReplicatedMessageStream,
        message_stream_subscription::ReplicatedMessageStreamSubscription,
        object_store::ReplicatedObjectStore,
        object_store_subscription::ReplicatedObjectStoreSubscription,
        service::{CurrentlySupportedListenerClasses, RhioService, RhioServiceStatus},
    },
    configuration::configmap::RhioConfigMapBuilder,
    rhio::{
        builders::{build_recommended_labels, build_rhio_statefulset, build_server_role_service},
        error::{
            ApplyRhioConfigSnafu, ApplyRoleBindingSnafu, ApplyRoleGroupStatefulSetSnafu,
            ApplyRoleServiceSnafu, ApplyServiceAccountSnafu, ApplyStatusSnafu, BuildConfigMapSnafu,
            BuildRbacResourcesSnafu, CreateClusterResourcesSnafu, DeleteOrphansSnafu,
            InvalidRhioServiceSnafu,
        },
    },
};

use crate::rhio::error::Result;
use futures::StreamExt;
use product_config::ProductConfigManager;
use rhio_http_api::{api::RhioApi, client::RhioApiClient, status::HealthStatus};
use snafu::{OptionExt, ResultExt};
use stackable_operator::kube::{api::ListParams, ResourceExt};
use stackable_operator::{client::Client, kube::runtime::Controller, namespace::WatchNamespace};
use stackable_operator::{
    cluster_resources::{ClusterResourceApplyStrategy, ClusterResources},
    commons::rbac::build_rbac_resources,
    kube::{
        core::{error_boundary, DeserializeGuard},
        runtime::{controller::Action, reflector::ObjectRef},
        Resource,
    },
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

use super::error::{Error, GetReplicatedMessageStreamsSnafu, ObjectHasNoNamespaceSnafu};

pub const APP_NAME: &str = "rhio";
pub const OPERATOR_NAME: &str = "rhio.hiro.io";
pub const RHIO_CONTROLLER_NAME: &str = "rhioservice";
pub const DOCKER_IMAGE_BASE_NAME: &str = "rhio";

pub struct Ctx {
    pub client: stackable_operator::client::Client,
    pub product_config: ProductConfigManager,
}

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

    let resolved_product_image = rhio_service.resolve_product_image();
    let client = &ctx.client;
    let rolegroup = rhio_service.server_rolegroup_ref();
    let recommended_labels = build_recommended_labels(
        &rhio_service,
        RHIO_CONTROLLER_NAME,
        &resolved_product_image.app_version_label,
        &rolegroup.role,
        &rolegroup.role_group,
    );

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

    if rhio_service.spec.cluster_config.listener_class
        != CurrentlySupportedListenerClasses::Disabled
    {
        cluster_resources
            .add(
                client,
                build_server_role_service(rhio_service, &rolegroup, recommended_labels.clone())?,
            )
            .await
            .context(ApplyRoleServiceSnafu)?;
    }

    let (rhio_configmap, config_map_hash) = make_builder(client.clone(), rhio_service)
        .await?
        .build(recommended_labels.clone())
        .context(BuildConfigMapSnafu)?;

    cluster_resources
        .add(client, rhio_configmap)
        .await
        .context(ApplyRhioConfigSnafu)?;

    let rhio_statefulset = build_rhio_statefulset(
        rhio_service,
        &resolved_product_image,
        &rolegroup,
        recommended_labels.clone(),
        &rbac_sa,
        config_map_hash,
    )?;

    let statefulset = cluster_resources
        .add(client, rhio_statefulset)
        .await
        .with_context(|_| ApplyRoleGroupStatefulSetSnafu {
            rolegroup: rolegroup.clone(),
        })?;

    let mut ss_cond_builder = StatefulSetConditionBuilder::default();
    ss_cond_builder.add(statefulset);

    let status = build_status(&mut requeue_duration, rhio_service, ss_cond_builder).await?;

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

async fn build_status(
    requeue_duration: &mut Duration,
    rhio_service: &RhioService,
    ss_cond_builder: StatefulSetConditionBuilder,
) -> Result<RhioServiceStatus> {
    let cluster_operation_cond_builder =
        ClusterOperationsConditionBuilder::new(&rhio_service.spec.cluster_operation);
    let endpoint = format!(
        "http://{}.default.svc.cluster.local:8080/health", // TODO cluster domain
        &rhio_service.metadata.name.clone().unwrap()
    );
    let maybe_health_status = RhioApiClient::new(endpoint).health().await;
    let health_status = match maybe_health_status {
        Ok(health_status) => health_status,
        Err(_e) => {
            *requeue_duration = Duration::from_secs(5);
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
    Ok(status)
}

pub async fn make_builder(
    client: Client,
    rhio_service: &RhioService,
) -> Result<RhioConfigMapBuilder> {
    let rms = client
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
        .filter(|stream| stream.metadata.namespace == rhio_service.metadata.namespace)
        .collect::<Vec<ReplicatedMessageStream>>();

    let ros = client
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
        .filter(|stream| stream.metadata.namespace == rhio_service.metadata.namespace)
        .collect::<Vec<ReplicatedObjectStore>>();

    let rmss = client
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
        .filter(|sub| sub.metadata.namespace == rhio_service.metadata.namespace)
        .collect::<Vec<ReplicatedMessageStreamSubscription>>();

    let ross = client
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
        .filter(|sub| sub.metadata.namespace == rhio_service.metadata.namespace)
        .collect::<Vec<ReplicatedObjectStoreSubscription>>();

    Ok(RhioConfigMapBuilder::from(
        rhio_service.clone(),
        rms,
        rmss,
        ros,
        ross,
    ))
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
