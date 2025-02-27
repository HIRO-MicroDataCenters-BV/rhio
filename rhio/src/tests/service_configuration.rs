use std::{sync::Arc, time::Duration};

use crate::{
    nats::client::fake::{
        blocking::BlockingClient,
        client::{FakeNatsClient, FakeNatsMessages},
        server::FakeNatsServer,
    },
    tests::{
        configuration::{configure_message_publisher, generate_nats_config, generate_rhio_config},
        fake_rhio_server::FakeRhioServer,
    },
    tracing::setup_tracing,
};
use anyhow::{Context, Result};
use p2panda_core::PrivateKey;
use rhio_config::configuration::Config;
use tokio::runtime::Builder;
use tracing::info;

#[test]
pub fn test_start_rhio_if_nats_not_available() -> Result<()> {
    setup_tracing(Some("=INFO".into()));

    let SingleNodeMessagingSetup {
        rhio_config,
        rhio_private_key,
        nats_server,
        test_runtime,
        ..
    } = create_single_node_messaging_setup()?;

    nats_server.enable_connection_error();

    let rhio = FakeRhioServer::try_start(rhio_config.clone(), rhio_private_key.clone())
        .context("Source RhioServer")?;

    test_runtime
        .block_on(async {
            nats_server
                .wait_for_connection_error(Duration::from_secs(10))
                .await
        })
        .context("waiting for connection errors")?;
    nats_server.disable_connection_error();

    test_runtime
        .block_on(async {
            nats_server
                .wait_for_connections(Duration::from_secs(5))
                .await
        })
        .context("waiting rhio reconnecting")?;

    rhio.discard()?;

    Ok(())
}

struct SingleNodeMessagingSetup {
    pub(crate) rhio_config: Config,
    pub(crate) rhio_private_key: PrivateKey,

    #[allow(dead_code)]
    pub(crate) nats_client: BlockingClient<FakeNatsClient, FakeNatsMessages>,
    pub(crate) nats_server: Arc<FakeNatsServer>,

    pub(crate) test_runtime: Arc<tokio::runtime::Runtime>,
}

fn create_single_node_messaging_setup() -> Result<SingleNodeMessagingSetup> {
    let nats_config = generate_nats_config();
    info!("nats config {:?}", nats_config);

    let mut rhio_config = generate_rhio_config(&nats_config, &None);
    let rhio_private_key = PrivateKey::new();

    info!("rhio source config {:?} ", rhio_config.node);

    configure_message_publisher(&mut rhio_config, "stream", "subject");

    let test_runtime = Arc::new(
        Builder::new_multi_thread()
            .enable_io()
            .enable_time()
            .thread_name("test-runtime")
            .worker_threads(5)
            .build()
            .expect("test tokio runtime"),
    );

    let nats_client = BlockingClient::new(
        FakeNatsClient::new(rhio_config.nats.clone()).context("Source FakeNatsClient")?,
        test_runtime.clone(),
    );

    let nats_server =
        FakeNatsServer::get_by_config(&nats_config).context("no fake NATS server exists")?;

    let setup = SingleNodeMessagingSetup {
        nats_server,
        nats_client,
        rhio_config,
        rhio_private_key,
        test_runtime,
    };

    Ok(setup)
}
