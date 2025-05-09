use super::error::{Error, GetRhioServiceEndpointSnafu, MultipleServicesInTheSameNamespaceSnafu};
use crate::configuration::error::Result;
use crate::{
    api::{
        message_stream::ReplicatedMessageStream,
        message_stream_subscription::ReplicatedMessageStreamSubscription,
        object_store::ReplicatedObjectStore,
        object_store_subscription::ReplicatedObjectStoreSubscription,
        service::{RhioService, RhioServiceStatus},
    },
    configuration::error::{
        ApplyStatusSnafu, GetRhioServiceSnafu, InvalidReplicatedResourceSnafu,
        ObjectHasNoNamespaceSnafu, RhioIsAbsentSnafu,
    },
    rhio::controller::OPERATOR_NAME,
};
use futures::StreamExt;
use rhio_http_api::api::RhioApi;
use rhio_http_api::client::RhioApiClient;
use rhio_http_api::status::HealthStatus;
use snafu::{OptionExt, ResultExt};
use stackable_operator::utils::cluster_info::KubernetesClusterInfo;
use stackable_operator::{client::Client, kube::runtime::Controller, namespace::WatchNamespace};
use stackable_operator::{
    client::GetApi,
    kube::{
        Resource, ResourceExt,
        api::ListParams,
        core::{DeserializeGuard, error_boundary},
        runtime::{controller::Action, reflector::ObjectRef},
    },
};
use stackable_operator::{
    k8s_openapi::api::core::v1::ConfigMap, kube::runtime::watcher,
    logging::controller::report_controller_reconciled,
};
use std::sync::Arc;
use std::time::Duration;
use tracing::warn;

pub const RMS_CONTROLLER_NAME: &str = "rms";
pub const RMSS_CONTROLLER_NAME: &str = "rmss";
pub const ROS_CONTROLLER_NAME: &str = "ros";
pub const ROSS_CONTROLLER_NAME: &str = "ross";

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
}

pub async fn reconcile_rms(
    rms_object: Arc<DeserializeGuard<ReplicatedMessageStream>>,
    ctx: Arc<Ctx>,
) -> Result<Action> {
    reconcile(rms_object, ctx, |rhio, stream| rhio.get_rms_status(stream)).await
}

pub async fn reconcile_rmss(
    rms_object: Arc<DeserializeGuard<ReplicatedMessageStreamSubscription>>,
    ctx: Arc<Ctx>,
) -> Result<Action> {
    reconcile(rms_object, ctx, |rhio, stream| rhio.get_rmss_status(stream)).await
}

pub async fn reconcile_ros(
    ros: Arc<DeserializeGuard<ReplicatedObjectStore>>,
    ctx: Arc<Ctx>,
) -> Result<Action> {
    reconcile(ros, ctx, |rhio, store| rhio.get_ros_status(store)).await
}

pub async fn reconcile_ross(
    ros: Arc<DeserializeGuard<ReplicatedObjectStoreSubscription>>,
    ctx: Arc<Ctx>,
) -> Result<Action> {
    reconcile(ros, ctx, |rhio, store| rhio.get_ross_status(store)).await
}

/// Reconciles the state of a given Kubernetes resource with the desired state.
///
/// This function is a generic reconciliation function that can be used to reconcile
/// different types of resources. It fetches the current state of the resource, compares
/// it with the desired state, and applies necessary changes to bring the resource to
/// the desired state.
///
/// # Arguments
///
/// * `config_object` - An `Arc` wrapped `DeserializeGuard` containing the resource to be reconciled.
/// * `ctx` - An `Arc` wrapped `Ctx` containing the client context.
/// * `get_status` - A closure that takes a reference to `RhioServiceStatus` and the resource `R`,
///   and returns the status `S` of the resource.
///
/// # Returns
///
/// A `Result` containing an `Action` that indicates the next action to be taken by the controller.
///
/// # Errors
///
/// This function will return an error if:
/// - The resource is invalid.
/// - The namespace of the resource cannot be determined.
/// - The list of `RhioService` resources cannot be fetched.
/// - There are multiple `RhioService` resources in the same namespace.
/// - The `RhioService` resource is absent.
/// - The `RhioService` resource has no status.
/// - The status of the resource cannot be applied.
///
async fn reconcile<R, S, GetStatusF>(
    config_object: Arc<DeserializeGuard<R>>,
    ctx: Arc<Ctx>,
    get_status: GetStatusF,
) -> Result<Action>
where
    R: Clone + std::fmt::Debug + serde::de::DeserializeOwned + Resource<DynamicType = ()> + GetApi,
    GetStatusF: Fn(&RhioServiceStatus, &R) -> S,
    S: std::fmt::Debug + serde::ser::Serialize,
{
    tracing::info!("Starting reconcile");

    let config_object = config_object
        .0
        .as_ref()
        .map_err(error_boundary::InvalidObject::clone)
        .context(InvalidReplicatedResourceSnafu)?;

    let client = &ctx.client;
    let cluster_info = &client.kubernetes_cluster_info;

    let services = client
        .list::<RhioService>(
            config_object
                .namespace()
                .as_deref()
                .context(ObjectHasNoNamespaceSnafu)?,
            &ListParams::default(),
        )
        .await
        .context(GetRhioServiceSnafu)?
        .into_iter()
        .filter(|stream| stream.metadata.namespace == config_object.meta().namespace)
        .collect::<Vec<RhioService>>();

    fail_if_multiple_services(&services)?;

    let rhio = services.first().context(RhioIsAbsentSnafu)?;

    let (rhio_status, maybe_requeue_duration) = get_rhio_status(rhio, cluster_info).await?;
    let config_object_status = get_status(&rhio_status, config_object);

    client
        .apply_patch_status(OPERATOR_NAME, config_object, &config_object_status)
        .await
        .context(ApplyStatusSnafu)?;

    if let Some(requeue_duration) = maybe_requeue_duration {
        Ok(Action::requeue(requeue_duration))
    } else {
        Ok(Action::requeue(RECONCILIATION_INTERVAL_DEFAULT))
    }
}

fn fail_if_multiple_services(services: &[RhioService]) -> Result<()> {
    if services.len() > 1 {
        return MultipleServicesInTheSameNamespaceSnafu {
            rhio: services.first().unwrap(),
        }
        .fail();
    }
    Ok(())
}

async fn get_rhio_status(
    rhio: &RhioService,
    cluster_info: &KubernetesClusterInfo,
) -> Result<(RhioServiceStatus, Option<Duration>)> {
    let endpoint = rhio
        .service_endpoint(cluster_info)
        .context(GetRhioServiceEndpointSnafu)?;
    let maybe_health_status = RhioApiClient::new(endpoint).health().await;
    let (status, requeue_duration) = match maybe_health_status {
        Ok(health_status) => (health_status, None),
        Err(e) => {
            warn!("Cannot fetch rhio service status {}", e);
            (HealthStatus::from(e), Some(RECONCILIATION_INTERVAL_ERROR))
        }
    };
    let rhio_status = RhioServiceStatus {
        conditions: vec![],
        status,
    };
    Ok((rhio_status, requeue_duration))
}

pub fn error_policy<R>(_obj: Arc<DeserializeGuard<R>>, error: &Error, _ctx: Arc<Ctx>) -> Action
where
    R: Clone + std::fmt::Debug + serde::de::DeserializeOwned + Resource<DynamicType = ()> + GetApi,
{
    match error {
        Error::InvalidReplicatedResource { .. } => Action::await_change(),
        _ => Action::requeue(RECONCILIATION_INTERVAL_ERROR),
    }
}

pub async fn create_rms_controller(client: &Client, namespace: WatchNamespace) {
    let rms_controller = Controller::new(
        namespace.get_api::<DeserializeGuard<ReplicatedMessageStream>>(client),
        watcher::Config::default(),
    );
    let rms_store = rms_controller.store();

    let rms_controller = rms_controller
        .owns(
            namespace.get_api::<ConfigMap>(client),
            watcher::Config::default(),
        )
        .watches(
            namespace.get_api::<DeserializeGuard<RhioService>>(client),
            watcher::Config::default(),
            move |rhio| {
                rms_store
                    .state()
                    .into_iter()
                    .filter(move |rms| {
                        let Ok(rms) = &rms.0 else {
                            return false;
                        };
                        let rhio_meta = rhio.meta();
                        rhio_meta.namespace == rms.meta().namespace
                    })
                    .map(|rms| ObjectRef::from_obj(&*rms))
            },
        )
        .shutdown_on_signal()
        .run(
            reconcile_rms,
            error_policy,
            Arc::new(Ctx {
                client: client.clone(),
            }),
        )
        .map(|res| {
            report_controller_reconciled(
                client,
                &format!("{RMS_CONTROLLER_NAME}.{OPERATOR_NAME}"),
                &res,
            );
        })
        .collect::<()>();
    rms_controller.await
}

pub async fn create_rmss_controller(client: &Client, namespace: WatchNamespace) {
    let rmss_controller = Controller::new(
        namespace.get_api::<DeserializeGuard<ReplicatedMessageStreamSubscription>>(client),
        watcher::Config::default(),
    );
    let rmss_store = rmss_controller.store();

    let rmss_controller = rmss_controller
        .owns(
            namespace.get_api::<ConfigMap>(client),
            watcher::Config::default(),
        )
        .watches(
            namespace.get_api::<DeserializeGuard<RhioService>>(client),
            watcher::Config::default(),
            move |rhio| {
                rmss_store
                    .state()
                    .into_iter()
                    .filter(move |rms| {
                        let Ok(rms) = &rms.0 else {
                            return false;
                        };
                        let rhio_meta = rhio.meta();
                        rhio_meta.namespace == rms.meta().namespace
                    })
                    .map(|rms| ObjectRef::from_obj(&*rms))
            },
        )
        .shutdown_on_signal()
        .run(
            reconcile_rmss,
            error_policy,
            Arc::new(Ctx {
                client: client.clone(),
            }),
        )
        .map(|res| {
            report_controller_reconciled(
                client,
                &format!("{RMSS_CONTROLLER_NAME}.{OPERATOR_NAME}"),
                &res,
            );
        })
        .collect::<()>();
    rmss_controller.await
}

pub async fn create_ros_controller(client: &Client, namespace: WatchNamespace) {
    let ros_controller = Controller::new(
        namespace.get_api::<DeserializeGuard<ReplicatedObjectStore>>(client),
        watcher::Config::default(),
    );
    let ros_store = ros_controller.store();

    let ros_controller = ros_controller
        .owns(
            namespace.get_api::<ConfigMap>(client),
            watcher::Config::default(),
        )
        .watches(
            namespace.get_api::<DeserializeGuard<RhioService>>(client),
            watcher::Config::default(),
            move |rhio| {
                ros_store
                    .state()
                    .into_iter()
                    .filter(move |ros| {
                        let Ok(ros) = &ros.0 else {
                            return false;
                        };
                        let rhio_meta = rhio.meta();
                        rhio_meta.namespace == ros.meta().namespace
                    })
                    .map(|ros| ObjectRef::from_obj(&*ros))
            },
        )
        .shutdown_on_signal()
        .run(
            reconcile_ros,
            error_policy,
            Arc::new(Ctx {
                client: client.clone(),
            }),
        )
        .map(|res| {
            report_controller_reconciled(
                client,
                &format!("{ROS_CONTROLLER_NAME}.{OPERATOR_NAME}"),
                &res,
            );
        })
        .collect::<()>();
    ros_controller.await
}

pub async fn create_ross_controller(client: &Client, namespace: WatchNamespace) {
    let ross_controller = Controller::new(
        namespace.get_api::<DeserializeGuard<ReplicatedObjectStoreSubscription>>(client),
        watcher::Config::default(),
    );
    let ross_store = ross_controller.store();

    let ross_controller = ross_controller
        .owns(
            namespace.get_api::<ConfigMap>(client),
            watcher::Config::default(),
        )
        .watches(
            namespace.get_api::<DeserializeGuard<RhioService>>(client),
            watcher::Config::default(),
            move |rhio| {
                ross_store
                    .state()
                    .into_iter()
                    .filter(move |ross| {
                        let Ok(ross) = &ross.0 else {
                            return false;
                        };
                        let rhio_meta = rhio.meta();
                        rhio_meta.namespace == ross.meta().namespace
                    })
                    .map(|ross| ObjectRef::from_obj(&*ross))
            },
        )
        .shutdown_on_signal()
        .run(
            reconcile_ross,
            error_policy,
            Arc::new(Ctx {
                client: client.clone(),
            }),
        )
        .map(|res| {
            report_controller_reconciled(
                client,
                &format!("{ROSS_CONTROLLER_NAME}.{OPERATOR_NAME}"),
                &res,
            );
        })
        .collect::<()>();
    ross_controller.await
}
