#![allow(clippy::result_large_err)]
use clap::{Parser, crate_description, crate_version};
use futures::FutureExt;
use rhio_operator::api::message_stream::ReplicatedMessageStream;
use rhio_operator::api::message_stream_subscription::ReplicatedMessageStreamSubscription;
use rhio_operator::api::object_store::ReplicatedObjectStore;
use rhio_operator::api::object_store_subscription::ReplicatedObjectStoreSubscription;
use rhio_operator::api::service::RhioService;
use rhio_operator::built_info;
use rhio_operator::cli::{Opts, RhioCommand};
use rhio_operator::configuration::controllers::{
    create_rms_controller, create_rmss_controller, create_ros_controller, create_ross_controller,
};
use rhio_operator::rhio::controller::{OPERATOR_NAME, create_rhio_controller};
use stackable_operator::cli::Command;
use stackable_operator::cli::{CommonOptions, RunArguments};
use stackable_operator::telemetry::Tracing;
use stackable_operator::{CustomResourceExt, client};

const RHIO_OPERATOR_PRODUCT_PROPERTIES: &str =
    "/etc/hiro/rhio-operator/config-spec/properties.yaml";
const RHIO_OPERATOR_LOCAL_PRODUCT_PROPERTIES: &str = "./config-spec/properties.yaml";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // We have to initialize default rustls crypto provider
    // because it is used from inside of stackable-operator -> kube-rs -> tokio-tls
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .expect("Failed to setup rustls default crypto provider [aws_lc_rs]");

    let opts = Opts::parse();
    match opts.cmd {
        RhioCommand::CreatePrivateKeySecret(cmd) => cmd.generate_secret()?,
        RhioCommand::CreateS3Secret(cmd) => cmd.generate_secret()?,
        RhioCommand::CreateNatsSecret(cmd) => cmd.generate_secret()?,
        RhioCommand::Framework(Command::Crd) => {
            RhioService::print_yaml_schema(built_info::PKG_VERSION)?;
            ReplicatedMessageStream::print_yaml_schema(built_info::PKG_VERSION)?;
            ReplicatedMessageStreamSubscription::print_yaml_schema(built_info::PKG_VERSION)?;
            ReplicatedObjectStore::print_yaml_schema(built_info::PKG_VERSION)?;
            ReplicatedObjectStoreSubscription::print_yaml_schema(built_info::PKG_VERSION)?;
        }
        RhioCommand::Framework(Command::Run(RunArguments {
            common:
                CommonOptions {
                    cluster_info,
                    telemetry,
                },
            product_config,
            watch_namespace,
            ..
        })) => {
            let _tracing_guard = Tracing::pre_configured(built_info::PKG_NAME, telemetry).init()?;

            tracing::info!(
                built_info.pkg_version = built_info::PKG_VERSION,
                built_info.git_version = built_info::GIT_VERSION,
                built_info.target = built_info::TARGET,
                built_info.built_time_utc = built_info::BUILT_TIME_UTC,
                built_info.rustc_version = built_info::RUSTC_VERSION,
                "Starting {description}",
                description = built_info::PKG_DESCRIPTION
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
                RHIO_OPERATOR_LOCAL_PRODUCT_PROPERTIES,
                RHIO_OPERATOR_PRODUCT_PROPERTIES,
            ])?;
            let client =
                client::initialize_operator(Some(OPERATOR_NAME.to_string()), &cluster_info).await?;
            let rhio_controller =
                create_rhio_controller(&client, product_config, watch_namespace.clone()).boxed();
            let rms_controller = create_rms_controller(&client, watch_namespace.clone()).boxed();
            let rmss_controller = create_rmss_controller(&client, watch_namespace.clone()).boxed();
            let ros_controller = create_ros_controller(&client, watch_namespace.clone()).boxed();
            let ross_controller = create_ross_controller(&client, watch_namespace.clone()).boxed();

            let futures = vec![
                rms_controller,
                rmss_controller,
                rhio_controller,
                ros_controller,
                ross_controller,
            ];
            futures::future::select_all(futures).await;
        }
    };

    Ok(())
}
