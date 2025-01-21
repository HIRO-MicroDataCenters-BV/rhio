#![allow(unused_imports, unused_variables)]
use std::sync::Arc;

use actix_web::web::Data;
use actix_web::{get, middleware, App, HttpRequest, HttpResponse, HttpServer, Responder};
use futures::StreamExt;
use product_config::ProductConfigManager;
use prometheus::{Encoder, TextEncoder};
use rhio_operator::api::service::RhioService;
use rhio_operator::rhio_controller;
use rhio_operator::state::State;
pub use rhio_operator::{self, telemetry};

use clap::{crate_description, crate_version, Parser};
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

mod built_info {
    // The file has been placed there by the build script.
    include!(concat!(env!("OUT_DIR"), "/built.rs"));
}

#[derive(clap::Parser)]
#[clap(about, author)]
struct Opts {
    #[clap(subcommand)]
    cmd: Command<RhioRun>,
}

#[derive(clap::Parser)]
struct RhioRun {
    #[clap(long, env)]
    rhio_clusterrole: String,
    #[clap(flatten)]
    common: ProductOperatorRun,
}

pub const APP_NAME: &str = "rhio";
pub const OPERATOR_NAME: &str = "rhio.hiro.io";
pub const RHIO_CONTROLLER_NAME: &str = "rhioservice";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let opts = Opts::parse();
    match opts.cmd {
        Command::Crd => RhioService::print_yaml_schema(built_info::PKG_VERSION)?,
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
                "deploy/config-spec/properties.yaml",
                "/etc/hiro/rhio-operator/config-spec/properties.yaml",
            ])?;
            let client =
                client::initialize_operator(Some(OPERATOR_NAME.to_string()), &cluster_info_opts)
                    .await?;
            create_controller(client, product_config, watch_namespace).await;
        }
    };

    Ok(())
}

pub struct ControllerConfig {
    pub broker_clusterrole: String,
}

pub async fn create_controller(
    client: Client,
    product_config: ProductConfigManager,
    namespace: WatchNamespace,
) {
    let kafka_controller = Controller::new(
        namespace.get_api::<DeserializeGuard<RhioService>>(&client),
        watcher::Config::default(),
    )
    .owns(
        namespace.get_api::<StatefulSet>(&client),
        watcher::Config::default(),
    )
    .owns(
        namespace.get_api::<Service>(&client),
        watcher::Config::default(),
    )
    .owns(
        namespace.get_api::<Listener>(&client),
        watcher::Config::default(),
    )
    .owns(
        namespace.get_api::<ConfigMap>(&client),
        watcher::Config::default(),
    )
    .owns(
        namespace.get_api::<ServiceAccount>(&client),
        watcher::Config::default(),
    )
    .owns(
        namespace.get_api::<RoleBinding>(&client),
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
            &client,
            &format!("{RHIO_CONTROLLER_NAME}.{OPERATOR_NAME}"),
            &res,
        );
    });

    kafka_controller.collect::<()>().await;
}
