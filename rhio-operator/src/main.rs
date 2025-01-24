use futures::{pin_mut, StreamExt};
use product_config::ProductConfigManager;
use rhio_operator::api::message_stream::ReplicatedMessageStream;
use rhio_operator::api::message_stream_subscription::ReplicatedMessageStreamSubscription;
use rhio_operator::api::object_store::ReplicatedObjectStore;
use rhio_operator::api::object_store_subscription::ReplicatedObjectStoreSubscription;
use rhio_operator::api::service::RhioService;
use rhio_operator::rhio_controller::{APP_NAME, OPERATOR_NAME, RHIO_CONTROLLER_NAME};
use rhio_operator::{built_info, rhio_controller, rms_controller};
use stackable_operator::kube::{Resource, ResourceExt};
use std::sync::Arc;

use clap::{crate_description, crate_version, Parser};
use stackable_operator::kube::runtime::reflector::ObjectRef;
use stackable_operator::{
    cli::{Command, ProductOperatorRun},
    client::{self, Client},
    commons::listener::Listener,
    k8s_openapi::api::{
        apps::v1::StatefulSet,
        core::v1::{ConfigMap, Service, ServiceAccount},
        rbac::v1::RoleBinding,
    },
    kube::core::DeserializeGuard,
    kube::runtime::{watcher, Controller},
    logging::controller::report_controller_reconciled,
    namespace::WatchNamespace,
    CustomResourceExt,
};

#[derive(clap::Parser)]
#[clap(about, author)]
struct Opts {
    #[clap(subcommand)]
    cmd: Command<RhioRun>,
}

#[derive(clap::Parser)]
struct RhioRun {
    #[clap(flatten)]
    common: ProductOperatorRun,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opts = Opts::parse();
    match opts.cmd {
        Command::Crd => {
            RhioService::print_yaml_schema(built_info::PKG_VERSION)?;
            ReplicatedMessageStream::print_yaml_schema(built_info::PKG_VERSION)?;
            ReplicatedMessageStreamSubscription::print_yaml_schema(built_info::PKG_VERSION)?;
            ReplicatedObjectStore::print_yaml_schema(built_info::PKG_VERSION)?;
            ReplicatedObjectStoreSubscription::print_yaml_schema(built_info::PKG_VERSION)?;
            // TODO make optional
            Listener::print_yaml_schema(built_info::PKG_VERSION)?;
        }
        Command::Run(RhioRun {
            common:
                ProductOperatorRun {
                    product_config,
                    watch_namespace,
                    tracing_target,
                    cluster_info_opts,
                },
            ..
        }) => {
            stackable_operator::logging::initialize_logging(
                "RHIO_OPERATOR_LOG",
                APP_NAME,
                tracing_target,
            );
            stackable_operator::utils::print_startup_string(
                crate_description!(),
                crate_version!(),
                built_info::GIT_VERSION,
                built_info::TARGET,
                built_info::BUILT_TIME_UTC,
                built_info::RUSTC_VERSION,
            );
            let product_config = product_config.load(&[
                "./config-spec/properties.yaml",
                "/etc/hiro/rhio-operator/config-spec/properties.yaml",
            ])?;
            let client =
                client::initialize_operator(Some(OPERATOR_NAME.to_string()), &cluster_info_opts)
                    .await?;
            let rhio_controller =
                create_rhio_controller(&client, product_config, watch_namespace.clone());
            let rms_controller = create_rms_controller(&client, watch_namespace);

            pin_mut!(rhio_controller, rms_controller);
            // kube-runtime's Controller will tokio::spawn each reconciliation, so this only concerns the internal watch machinery
            futures::future::select(rhio_controller, rms_controller).await;
        }
    };

    Ok(())
}

pub struct ControllerConfig {
    pub broker_clusterrole: String,
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
            rhio_controller::reconcile_rhio,
            rhio_controller::error_policy,
            Arc::new(rhio_controller::Ctx {
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
            rms_controller::reconcile_rms,
            rms_controller::error_policy,
            Arc::new(rms_controller::Ctx {
                client: client.clone(),
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
    rms_controller.await
}
