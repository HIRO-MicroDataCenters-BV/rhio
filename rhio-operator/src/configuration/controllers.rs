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
        ObjectHasNoNamespaceSnafu, RhioIsAbsentSnafu, RhioServiceHasNoStatusSnafu,
    },
    rhio::controller::OPERATOR_NAME,
};
use futures::StreamExt;
use snafu::{OptionExt, ResultExt};
use stackable_operator::{client::Client, kube::runtime::Controller, namespace::WatchNamespace};
use stackable_operator::{
    client::GetApi,
    kube::{
        api::ListParams,
        core::{error_boundary, DeserializeGuard},
        runtime::{controller::Action, reflector::ObjectRef},
        Resource, ResourceExt,
    },
    time::Duration,
};
use stackable_operator::{
    k8s_openapi::api::core::v1::ConfigMap, kube::runtime::watcher,
    logging::controller::report_controller_reconciled,
};
use std::sync::Arc;

use super::error::{Error, MultipleServicesInTheSameNamespaceSnafu};

pub const RMS_CONTROLLER_NAME: &str = "rms";
pub const RMSS_CONTROLLER_NAME: &str = "rmss";
pub const ROS_CONTROLLER_NAME: &str = "ros";
pub const ROSS_CONTROLLER_NAME: &str = "ross";

pub const RECONCILIATION_INTERVAL_DEFAULT: Duration = Duration::from_secs(60);
pub const RECONCILIATION_INTERVAL_ERROR: Duration = Duration::from_secs(5);

pub struct Ctx {
    pub client: stackable_operator::client::Client,
}

pub async fn reconcile_rms(
    rms_object: Arc<DeserializeGuard<ReplicatedMessageStream>>,
    ctx: Arc<Ctx>,
) -> Result<Action> {
    reconcile(rms_object, ctx, |rhio, stream| rhio.stream_status(stream)).await
}

pub async fn reconcile_rmss(
    rms_object: Arc<DeserializeGuard<ReplicatedMessageStreamSubscription>>,
    ctx: Arc<Ctx>,
) -> Result<Action> {
    reconcile(rms_object, ctx, |rhio, stream| {
        rhio.stream_subscription_status(stream)
    })
    .await
}

pub async fn reconcile_ros(
    ros: Arc<DeserializeGuard<ReplicatedObjectStore>>,
    ctx: Arc<Ctx>,
) -> Result<Action> {
    reconcile(ros, ctx, |rhio, store| rhio.store_status(store)).await
}

pub async fn reconcile_ross(
    ros: Arc<DeserializeGuard<ReplicatedObjectStoreSubscription>>,
    ctx: Arc<Ctx>,
) -> Result<Action> {
    reconcile(ros, ctx, |rhio, store| {
        rhio.store_subscription_status(store)
    })
    .await
}

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
    let rhio_status = rhio.status.as_ref().context(RhioServiceHasNoStatusSnafu {
        rhio: ObjectRef::from(rhio),
    })?;
    let config_object_status = get_status(rhio_status, config_object);

    client
        .apply_patch_status(OPERATOR_NAME, config_object, &config_object_status)
        .await
        .context(ApplyStatusSnafu)?;

    Ok(Action::requeue(*RECONCILIATION_INTERVAL_DEFAULT))
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

pub fn error_policy<R>(_obj: Arc<DeserializeGuard<R>>, error: &Error, _ctx: Arc<Ctx>) -> Action
where
    R: Clone + std::fmt::Debug + serde::de::DeserializeOwned + Resource<DynamicType = ()> + GetApi,
{
    match error {
        Error::InvalidReplicatedResource { .. } => Action::await_change(),
        _ => Action::requeue(*RECONCILIATION_INTERVAL_ERROR),
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
