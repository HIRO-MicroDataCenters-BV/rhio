use crate::configuration::secret::Secret;
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
        builders::{build_recommended_labels, build_service, build_statefulset},
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
use rhio_config::configuration::NatsCredentials;
use rhio_http_api::{api::RhioApi, client::RhioApiClient, status::HealthStatus};
use s3::creds::Credentials;
use snafu::{OptionExt, ResultExt};
use stackable_operator::client::GetApi;
use stackable_operator::kube::api::ListParams;
use stackable_operator::kube::ResourceExt;
use stackable_operator::kvp::Labels;
use stackable_operator::utils::cluster_info::KubernetesClusterInfo;
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
use tracing::warn;

use super::error::{Error, GetRhioServiceEndpointSnafu, GetSecretSnafu, ListResourceSnafu};

pub const APP_NAME: &str = "rhio";
pub const OPERATOR_NAME: &str = "rhio.hiro.io";
pub const RHIO_CONTROLLER_NAME: &str = "rhioservice";
pub const DOCKER_IMAGE_BASE_NAME: &str = "rhio";

// The default interval for reconciliation loops. This interval is used to
// periodically requeue the reconciliation process to ensure that the state
// of the resources is continuously monitored and updated if necessary.
pub const RECONCILIATION_INTERVAL_DEFAULT: Duration = Duration::from_secs(60);

// The interval for reconciliation loops when an error occurs. This interval
// is used to requeue the reconciliation process more frequently in case of
// errors, allowing for quicker recovery and resolution of issues.
pub const RECONCILIATION_INTERVAL_ERROR: Duration = Duration::from_secs(5);

pub struct Ctx {
    pub client: stackable_operator::client::Client,
    pub product_config: ProductConfigManager,
}

pub async fn reconcile_rhio(
    rhio: Arc<DeserializeGuard<RhioService>>,
    ctx: Arc<Ctx>,
) -> Result<Action> {
    tracing::info!("Starting reconcile");

    let rhio: &RhioService = rhio
        .0
        .as_ref()
        .map_err(error_boundary::InvalidObject::clone)
        .context(InvalidRhioServiceSnafu)?;

    let resolved_product_image = rhio.resolve_product_image();
    let client = &ctx.client;
    let cluster_info = &client.kubernetes_cluster_info;
    let rolegroup = rhio.server_rolegroup_ref();
    let recommended_labels = build_recommended_labels(
        rhio,
        RHIO_CONTROLLER_NAME,
        &resolved_product_image.app_version_label,
        &rolegroup.role,
        &rolegroup.role_group,
    );
    let role_group_selector =
        Labels::role_group_selector(rhio, APP_NAME, &rolegroup.role, &rolegroup.role_group)
            .context(BuildLabelSnafu)?;

    let mut cluster_resources = ClusterResources::new(
        APP_NAME,
        OPERATOR_NAME,
        RHIO_CONTROLLER_NAME,
        &rhio.object_ref(&()),
        ClusterResourceApplyStrategy::from(&rhio.spec.cluster_operation),
    )
    .context(CreateClusterResourcesSnafu)?;

    let (rbac_sa, rbac_rolebinding) = build_rbac_resources(
        rhio,
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

    if rhio.spec.cluster_config.listener_class != CurrentlySupportedListenerClasses::Disabled {
        cluster_resources
            .add(
                client,
                build_service(
                    rhio,
                    recommended_labels.clone(),
                    role_group_selector.clone(),
                )?,
            )
            .await
            .context(ApplyRoleServiceSnafu)?;
    }

    let (rhio_configmap, config_map_hash) = make_builder(client.clone(), rhio)
        .await?
        .build(recommended_labels.clone())
        .context(BuildConfigMapSnafu)?;

    cluster_resources
        .add(client, rhio_configmap)
        .await
        .context(ApplyRhioConfigSnafu)?;

    let rhio_statefulset = build_statefulset(
        rhio,
        &resolved_product_image,
        &rolegroup,
        recommended_labels.clone(),
        role_group_selector,
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

    let (status, maybe_duration) = build_status(rhio, cluster_info, ss_cond_builder).await?;

    cluster_resources
        .delete_orphaned_resources(client)
        .await
        .context(DeleteOrphansSnafu)?;

    client
        .apply_patch_status(OPERATOR_NAME, rhio, &status)
        .await
        .context(ApplyStatusSnafu)?;

    if let Some(duration) = maybe_duration {
        Ok(Action::requeue(duration))
    } else {
        Ok(Action::await_change())
    }
}

async fn build_status(
    rhio: &RhioService,
    cluster_info: &KubernetesClusterInfo,
    ss_cond_builder: StatefulSetConditionBuilder,
) -> Result<(RhioServiceStatus, Option<Duration>)> {
    let endpoint = rhio
        .service_endpoint(cluster_info)
        .context(GetRhioServiceEndpointSnafu)?;
    let maybe_health_status = RhioApiClient::new(endpoint).health().await;
    let (health_status, requeue_duration) = match maybe_health_status {
        Ok(health_status) => (health_status, None),
        Err(e) => {
            warn!("Cannot fetch rhio service status {}", e);
            (HealthStatus::from(e), Some(RECONCILIATION_INTERVAL_ERROR))
        }
    };

    let cluster_operation_cond_builder =
        ClusterOperationsConditionBuilder::new(&rhio.spec.cluster_operation);
    let status = RhioServiceStatus {
        conditions: compute_conditions(rhio, &[&ss_cond_builder, &cluster_operation_cond_builder]),
        status: health_status,
    };
    Ok((status, requeue_duration))
}

/// Constructs a `RhioConfigMapBuilder` for the given `RhioService`.
///
/// This function gathers necessary resources and secrets required to build the configuration map
/// for the `RhioService`. It fetches the following resources:
/// - ReplicatedMessageStream
/// - ReplicatedMessageStreamSubscription
/// - ReplicatedObjectStore
/// - ReplicatedObjectStoreSubscription
///
/// Additionally, it fetches the NATS and S3 credentials secrets if they are specified in the
/// `RhioService` configuration.
///
/// # Arguments
///
/// * `client` - A reference to the Kubernetes client.
/// * `rhio` - A reference to the `RhioService` instance.
///
/// # Returns
///
/// A `Result` containing the `RhioConfigMapBuilder` if successful, or an error if any of the
/// resource or secret fetching operations fail.
///
pub async fn make_builder(client: Client, rhio: &RhioService) -> Result<RhioConfigMapBuilder> {
    let namespace = rhio.get_namespace();

    let rms = list_resources::<ReplicatedMessageStream>(&client, rhio, namespace).await?;
    let rmss =
        list_resources::<ReplicatedMessageStreamSubscription>(&client, rhio, namespace).await?;
    let ros = list_resources::<ReplicatedObjectStore>(&client, rhio, namespace).await?;
    let ross =
        list_resources::<ReplicatedObjectStoreSubscription>(&client, rhio, namespace).await?;

    let nats_secret = get_secret::<NatsCredentials>(
        &client,
        &rhio.spec.configuration.nats.credentials_secret,
        namespace,
    )
    .await?;

    let s3_secret_name: Option<String> = rhio
        .spec
        .configuration
        .s3
        .as_ref()
        .and_then(|s3_conf| s3_conf.credentials_secret.clone());

    let s3_secret = get_secret::<Credentials>(&client, &s3_secret_name, namespace).await?;

    Ok(RhioConfigMapBuilder::from(
        rhio.clone(),
        rms,
        rmss,
        ros,
        ross,
        nats_secret,
        s3_secret,
    ))
}

async fn list_resources<T>(
    client: &Client,
    rhio: &RhioService,
    namespace: &<T as GetApi>::Namespace,
) -> Result<Vec<T>>
where
    T: Clone
        + std::fmt::Debug
        + serde::de::DeserializeOwned
        + Resource
        + stackable_operator::client::GetApi,
    <T as Resource>::DynamicType: Default,
{
    let resources = client
        .list::<T>(namespace, &ListParams::default())
        .await
        .context(ListResourceSnafu {
            rhio: ObjectRef::from_obj(rhio),
        })?
        .into_iter()
        .collect::<Vec<T>>();
    Ok(resources)
}

async fn get_secret<T>(
    client: &Client,
    secret_name: &Option<String>,
    secret_namespace: &str,
) -> Result<Option<Secret<T>>>
where
    T: serde::Serialize + for<'de> serde::Deserialize<'de>,
{
    if let Some(name) = secret_name.as_ref() {
        Secret::<T>::fetch(client, name, secret_namespace)
            .await
            .context(GetSecretSnafu {
                secret_name: name.to_owned(),
                secret_namespace: secret_namespace.to_string(),
            })
            .map(|r| r.into())
    } else {
        Ok(None)
    }
}

pub fn error_policy(
    _obj: Arc<DeserializeGuard<RhioService>>,
    error: &Error,
    _ctx: Arc<Ctx>,
) -> Action {
    match error {
        Error::InvalidRhioService { .. } => Action::await_change(),
        _ => Action::requeue(RECONCILIATION_INTERVAL_ERROR),
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
