use futures::FutureExt;
use rhio_operator::api::message_stream::ReplicatedMessageStream;
use rhio_operator::api::message_stream_subscription::ReplicatedMessageStreamSubscription;
use rhio_operator::api::object_store::ReplicatedObjectStore;
use rhio_operator::api::object_store_subscription::ReplicatedObjectStoreSubscription;
use rhio_operator::api::service::RhioService;
use rhio_operator::built_info;
use rhio_operator::rhio_controller::{create_rhio_controller, APP_NAME, OPERATOR_NAME};
use rhio_operator::stream_controller::{
    create_rms_controller, create_rmss_controller, create_ros_controller, create_ross_controller,
};

use clap::{crate_description, crate_version, Parser};
use stackable_operator::{
    cli::{Command, ProductOperatorRun},
    client,
    commons::listener::Listener,
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
