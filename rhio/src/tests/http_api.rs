use std::sync::Arc;

use super::fake_rhio_server::FakeRhioServer;
use crate::{
    tests::configuration::{generate_nats_config, generate_rhio_config},
    tracing::setup_tracing,
};
use anyhow::{Context, Result};
use p2panda_core::PrivateKey;
use rhio_http_api::{
    blocking::BlockingClient,
    client::RhioApiClient,
    status::{HealthStatus, ServiceStatus},
};
use tokio::runtime::Builder;
use tracing::info;

#[test]
pub fn test_http_api() -> Result<()> {
    setup_tracing(Some("=INFO".into()));

    let SingleServerSetup { rhio, http_api } = create_setup()?;

    let status = http_api.health()?;
    assert_eq!(
        HealthStatus {
            status: ServiceStatus::Running,
            msg: None,
            ..HealthStatus::default()
        },
        status
    );

    rhio.discard()?;
    Ok(())
}

struct SingleServerSetup {
    rhio: FakeRhioServer,
    http_api: BlockingClient<RhioApiClient>,
}

fn create_setup() -> Result<SingleServerSetup> {
    let nats_config = generate_nats_config();
    info!("nats config {:?}", nats_config);

    let rhio_config = generate_rhio_config(&nats_config, &None);
    let rhio_private_key = PrivateKey::new();

    info!("rhio config {:?} ", rhio_config.node);

    let test_runtime = Arc::new(
        Builder::new_multi_thread()
            .enable_io()
            .enable_time()
            .thread_name("test-runtime")
            .worker_threads(5)
            .build()
            .expect("test tokio runtime"),
    );

    let http_api = BlockingClient::new(
        RhioApiClient::new(format!(
            "http://127.0.0.1:{}",
            rhio_config.node.http_bind_port
        )),
        test_runtime.clone(),
    );
    let rhio = FakeRhioServer::try_start(rhio_config.clone(), rhio_private_key.clone())
        .context("RhioServer")?;

    let setup = SingleServerSetup { rhio, http_api };

    Ok(setup)
}
