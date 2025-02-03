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
pub fn test_health_status() -> Result<()> {
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

#[test]
pub fn test_metrics() -> Result<()> {
    setup_tracing(Some("=INFO".into()));

    let SingleServerSetup { rhio, http_api } = create_setup()?;

    let metrics = http_api.metrics()?;
    assert_eq!("", metrics);

    rhio.discard()?;
    Ok(())
}

struct SingleServerSetup {
    rhio: FakeRhioServer,
    http_api: BlockingClient<RhioApiClient>,
}

/// Creates a setup for testing with a single server instance.
///
/// This function generates the necessary configurations for NATS and Rhio,
/// initializes a Tokio runtime, and starts a fake Rhio server. It also
/// creates an HTTP API client for interacting with the Rhio server.
///
/// # Returns
///
/// A `Result` containing a `SingleServerSetup` struct with the initialized
/// Rhio server and HTTP API client, or an error if the setup fails.
///
/// # Errors
///
/// This function will return an error if the Rhio server fails to start or
/// if there is an issue with the configurations.
///
/// # Example
///
/// ```rust
/// let setup = create_setup()?;
/// ```
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
            .worker_threads(2)
            .build()
            .expect("test tokio runtime"),
    );

    let http_api = BlockingClient::new(
        RhioApiClient::new(format!(
            "http://127.0.0.1:{}",
            rhio_config.node.http_bind_port
        )),
        test_runtime,
    );
    let rhio = FakeRhioServer::try_start(rhio_config.clone(), rhio_private_key.clone())
        .context("RhioServer")?;

    let setup = SingleServerSetup { rhio, http_api };

    Ok(setup)
}
