use crate::{
    tests::configuration::{
        configure_blob_subscription, configure_network, generate_nats_config, generate_rhio_config,
        generate_s3_config,
    },
    tracing::setup_tracing,
};
use anyhow::{anyhow, bail, Context, Result};
use p2panda_core::PrivateKey;
use rand::Rng;
use rhio_config::configuration::S3Config;
use s3_server::FakeS3Server;
use s3s::auth::SimpleAuth;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::runtime::{Builder, Runtime};
use tracing::{debug, info};
use url::Url;

use super::fake_rhio_server::FakeRhioServer;

#[test]
pub fn test_e2e_blob_replication() -> Result<()> {
    setup_tracing(Some("=INFO".into()));

    let TwoClusterBlobSetup {
        rhio_source,
        rhio_target,
        s3_source,
        s3_target,
    } = create_two_node_blob_setup()?;
    info!("environment started");

    let mut rng = rand::thread_rng();
    let source_bytes: Vec<u8> = (0..128).map(|_| rng.gen()).collect();

    s3_source.put_bytes("source-bucket", "test.txt", &source_bytes)?;

    wait_for_condition(Duration::from_secs(10), || {
        s3_target
            .exists("target-bucket", "test.txt")
            .context("Waiting for file")
    })?;

    let target_bytes = s3_target.get_bytes("target-bucket", "test.txt")?;

    assert_eq!(source_bytes, target_bytes);

    rhio_source.discard()?;
    rhio_target.discard()?;
    s3_source.discard();
    s3_target.discard();
    Ok(())
}

fn wait_for_condition<F>(timeout: Duration, condition: F) -> Result<()>
where
    F: Fn() -> Result<bool>,
{
    let start = Instant::now();
    while Instant::now().duration_since(start) < timeout {
        if condition()? {
            return Ok(());
        }
        std::thread::sleep(Duration::from_secs(1));
    }
    bail!("timeout waiting condition")
}

/// A structure representing the setup for a two-cluster blob storage system.
///
/// This setup includes two instances of `FakeRhioServer` and two instances of
/// `FakeS3Server`. The `FakeRhioServer` instances are configured to communicate
/// with each other and the `FakeS3Server` instances are configured to handle
/// blob storage and retrieval.
///
pub struct TwoClusterBlobSetup {
    pub(crate) rhio_source: FakeRhioServer,
    pub(crate) rhio_target: FakeRhioServer,
    pub(crate) s3_source: FakeS3Server,
    pub(crate) s3_target: FakeS3Server,
}

/// Creates a two-node blob storage setup for testing purposes.
///
/// This function sets up two instances of `FakeRhioServer` and two instances of
/// `FakeS3Server`. It configures the necessary network and blob subscription settings
/// for the nodes to communicate with each other and handle blob storage and retrieval.
///
/// # Returns
///
/// A `Result` containing a `TwoClusterBlobSetup` structure if the setup is successful,
/// or an `anyhow::Error` if an error occurs during the setup.
///
/// # Example
///
/// ```rust
/// let setup = create_two_node_blob_setup().expect("Failed to create two-node blob setup");
/// ```
///
/// # Errors
///
/// This function will return an error if there is an issue creating the S3 servers
/// or starting the `FakeRhioServer` instances.
pub fn create_two_node_blob_setup() -> Result<TwoClusterBlobSetup> {
    let nats_source_config = generate_nats_config();
    let nats_target_config = generate_nats_config();
    info!("nats source config {:?}", nats_source_config);
    info!("nats target config {:?}", nats_target_config);

    let s3_source_config = generate_s3_config();
    let s3_target_config = generate_s3_config();
    info!("s3 source config {:?}", s3_source_config);
    info!("s3 target config {:?}", s3_target_config);

    let mut rhio_source_config =
        generate_rhio_config(&nats_source_config, &Some(s3_source_config.clone()));
    let rhio_source_private_key = PrivateKey::new();

    let mut rhio_target_config =
        generate_rhio_config(&nats_target_config, &Some(s3_target_config.clone()));
    let rhio_target_private_key = PrivateKey::new();

    configure_network(vec![
        (&mut rhio_source_config, &rhio_source_private_key),
        (&mut rhio_target_config, &rhio_target_private_key),
    ]);

    info!("rhio source config {:?} ", rhio_source_config.node);
    info!("rhio target config {:?} ", rhio_target_config.node);

    configure_blob_subscription(
        &mut rhio_source_config,
        &rhio_source_private_key.public_key(),
        &mut rhio_target_config,
        &"source-bucket",
        &"target-bucket",
    );

    let test_runtime = Arc::new(
        Builder::new_multi_thread()
            .enable_io()
            .enable_time()
            .thread_name("test-runtime")
            .worker_threads(3)
            .build()
            .expect("test tokio runtime"),
    );

    let s3_source = new_s3_server(&s3_source_config, test_runtime.clone())?;
    s3_source.create_bucket("source-bucket")?;

    let s3_target = new_s3_server(&s3_target_config, test_runtime.clone())?;
    s3_target.create_bucket("target-bucket")?;

    let rhio_source =
        FakeRhioServer::try_start(rhio_source_config.clone(), rhio_source_private_key.clone())
            .context("Source RhioServer")?;
    let rhio_target =
        FakeRhioServer::try_start(rhio_target_config.clone(), rhio_target_private_key.clone())
            .context("Target RhioServer")?;

    let setup = TwoClusterBlobSetup {
        rhio_source,
        rhio_target,
        s3_source,
        s3_target,
    };

    Ok(setup)
}

fn new_s3_server(s3_config: &S3Config, runtime: Arc<Runtime>) -> Result<FakeS3Server> {
    let maybe_auth = if let Some(credentials) = &s3_config.credentials {
        match (&credentials.access_key, &credentials.secret_key) {
            (Some(access_key), Some(secret_key)) => {
                Some(SimpleAuth::from_single(access_key, secret_key.as_str()))
            }
            _ => None,
        }
    } else {
        None
    };

    debug!("s3 server {} has auth {:?}", s3_config.endpoint, maybe_auth);

    let url: Url = s3_config
        .endpoint
        .parse()
        .context(format!("Invalid endpoint address {}", s3_config.endpoint))?;

    let host = url
        .host()
        .ok_or(anyhow!("s3 url does not have host"))?
        .to_string();
    let port = url
        .port()
        .ok_or(anyhow!("s3 url does not have port specified"))?;

    let s3 = FakeS3Server::new(host, port, maybe_auth, runtime.clone())
        .context(format!("Creating FakeS3Server {}", s3_config.endpoint))?;
    Ok(s3)
}
