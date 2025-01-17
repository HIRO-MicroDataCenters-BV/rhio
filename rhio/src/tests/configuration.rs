use anyhow::Context;
use once_cell::sync::Lazy;
use p2panda_core::{PrivateKey, PublicKey};
use rhio_core::Subject;
use std::{
    path::PathBuf,
    str::FromStr,
    sync::{
        atomic::{AtomicU16, Ordering},
        Arc,
    },
};
use tokio::runtime::Builder;
use tracing::info;

use crate::{
    config::{
        Config, LocalNatsSubject, NatsConfig, PublishConfig, RemoteNatsSubject, S3Config,
        SubscribeConfig,
    },
    nats::client::fake::{
        blocking::BlockingClient,
        client::{FakeNatsClient, FakeNatsMessages},
    },
};

use super::fake_rhio_server::FakeRhioServer;
use anyhow::Result;

static TEST_INSTANCE_HTTP_PORT: Lazy<AtomicU16> = Lazy::new(|| AtomicU16::new(8080));
static TEST_INSTANCE_RHIO_PORT: Lazy<AtomicU16> = Lazy::new(|| AtomicU16::new(31000));
static TEST_INSTANCE_NATS_PORT: Lazy<AtomicU16> = Lazy::new(|| AtomicU16::new(4222));

/// A structure representing the setup for a two-cluster messaging system.
///
/// This setup includes two instances of `FakeRhioServer` and two instances of
/// `BlockingClient` with `FakeNatsClient` and `FakeNatsMessages`.
///
pub struct TwoClusterMessagingSetup {
    pub(crate) rhio_source: FakeRhioServer,
    pub(crate) rhio_target: FakeRhioServer,

    pub(crate) nats_source: BlockingClient<FakeNatsClient, FakeNatsMessages>,
    pub(crate) nats_target: BlockingClient<FakeNatsClient, FakeNatsMessages>,
}

/// Creates a two-node messaging setup for testing purposes.
///
/// This function sets up two instances of `FakeRhioServer` and two instances of
/// `BlockingClient` with `FakeNatsClient` and `FakeNatsMessages`. It configures
/// the necessary network and message subscription settings for the nodes to
/// communicate with each other.
///
/// # Returns
///
/// A `Result` containing a `TwoClusterMessagingSetup` structure if the setup
/// is successful, or an `anyhow::Error` if an error occurs during the setup.
///
/// # Example
///
/// ```rust
/// let setup = create_two_node_messaging_setup().expect("Failed to create two-node messaging setup");
/// ```
///
/// # Errors
///
/// This function will return an error if there is an issue creating the NATS clients
/// or starting the `FakeRhioServer` instances.
pub fn create_two_node_messaging_setup() -> Result<TwoClusterMessagingSetup> {
    let nats_source_config = generate_nats_config();
    let nats_target_config = generate_nats_config();
    info!("nats source config {:?}", nats_source_config);
    info!("nats target config {:?}", nats_target_config);

    let mut rhio_source_config = generate_rhio_config(&nats_source_config, &None);
    let rhio_source_private_key = PrivateKey::new();

    let mut rhio_target_config = generate_rhio_config(&nats_target_config, &None);
    let rhio_target_private_key = PrivateKey::new();

    configure_network(vec![
        (&mut rhio_source_config, &rhio_source_private_key),
        (&mut rhio_target_config, &rhio_target_private_key),
    ]);

    info!("rhio source config {:?} ", rhio_source_config.node);
    info!("rhio target config {:?} ", rhio_target_config.node);

    configure_message_subscription(
        &mut rhio_source_config,
        &rhio_source_private_key.public_key(),
        &mut rhio_target_config,
        &"test-stream",
        &"test.subject1",
    );

    let test_runtime = Arc::new(
        Builder::new_multi_thread()
            .enable_io()
            .enable_time()
            .thread_name("test-runtime")
            .worker_threads(5)
            .build()
            .expect("test tokio runtime"),
    );

    let nats_source = BlockingClient::new(
        FakeNatsClient::new(rhio_source_config.nats.clone()).context("Source FakeNatsClient")?,
        test_runtime.clone(),
    );
    let nats_target = BlockingClient::new(
        FakeNatsClient::new(rhio_target_config.nats.clone()).context("Target FakeNatsClient")?,
        test_runtime,
    );

    let rhio_source =
        FakeRhioServer::try_start(rhio_source_config.clone(), rhio_source_private_key.clone())
            .context("Source RhioServer")?;
    let rhio_target =
        FakeRhioServer::try_start(rhio_target_config.clone(), rhio_target_private_key.clone())
            .context("Target RhioServer")?;

    let setup = TwoClusterMessagingSetup {
        rhio_source,
        rhio_target,
        nats_source,
        nats_target,
    };

    Ok(setup)
}

pub fn generate_rhio_config(nats_config: &NatsConfig, s3_config: &Option<S3Config>) -> Config {
    let http_port = TEST_INSTANCE_HTTP_PORT.fetch_add(1, Ordering::SeqCst);
    let rhio_port = TEST_INSTANCE_RHIO_PORT.fetch_add(1, Ordering::SeqCst);

    let mut config = Config::default();
    config.s3 = s3_config.clone();
    config.nats = nats_config.clone();
    config.node.bind_port = rhio_port;
    config.node.http_bind_port = http_port;
    config.node.known_nodes = vec![];
    config.node.private_key_path = PathBuf::from("/tmp/rhio_private_key");
    config.node.network_id = "test".to_string();
    config.log_level = Some("=INFO".to_string());
    config
}

pub fn generate_nats_config() -> NatsConfig {
    let nats_port = TEST_INSTANCE_NATS_PORT.fetch_add(1, Ordering::SeqCst);
    NatsConfig {
        endpoint: format!("nats://localhost:{}", nats_port),
        credentials: None,
    }
}

pub fn configure_network(nodes: Vec<(&mut Config, &PrivateKey)>) {
    let mut nodes = nodes;
    for i in 0..nodes.len() {
        let mut known_nodes = vec![];
        for j in 0..nodes.len() {
            if i != j {
                let (node_config, private_key) = &nodes[j];
                known_nodes.push(crate::config::KnownNode {
                    public_key: private_key.public_key(),
                    direct_addresses: vec![format!("127.0.0.1:{}", node_config.node.bind_port)],
                });
            }
        }
        nodes[i].0.node.known_nodes = known_nodes;
    }
}

/// Configures message subscription between a publisher and a subscriber.
///
/// This function sets up the necessary configurations for a publisher to publish messages
/// to a specific subject and stream, and for a subscriber to subscribe to those messages.
///
/// # Arguments
///
/// * `publisher` - A mutable reference to the configuration of the publisher node.
/// * `publisher_pub_key` - The public key of the publisher node.
/// * `subscriber` - A mutable reference to the configuration of the subscriber node.
/// * `stream` - The name of the stream to which messages will be published.
/// * `subject` - The subject under which messages will be published and subscribed to.
///
///
/// # Example
///
/// ```rust
/// let mut publisher_config = Config::default();
/// let publisher_private_key = PrivateKey::new();
/// let mut subscriber_config = Config::default();
///
/// configure_message_subscription(
///     &mut publisher_config,
///     &publisher_private_key.public_key(),
///     &mut subscriber_config,
///     "test-stream",
///     "test.subject1",
/// );
/// ```

pub fn configure_message_subscription(
    publisher: &mut Config,
    publisher_pub_key: &PublicKey,
    subscriber: &mut Config,
    stream: &str,
    subject: &str,
) {
    if publisher.publish.is_none() {
        publisher.publish = Some(PublishConfig {
            s3_buckets: vec![],
            nats_subjects: vec![],
        });
    }
    if subscriber.subscribe.is_none() {
        subscriber.subscribe = Some(SubscribeConfig {
            s3_buckets: vec![],
            nats_subjects: vec![],
        });
    }
    if let Some(publish_config) = &mut publisher.publish {
        publish_config.nats_subjects.push(LocalNatsSubject {
            subject: Subject::from_str(subject).unwrap(),
            stream_name: stream.into(),
        });
    }

    if let Some(subscriber_config) = &mut subscriber.subscribe {
        subscriber_config.nats_subjects.push(RemoteNatsSubject {
            public_key: publisher_pub_key.clone(),
            subject: Subject::from_str(subject).unwrap(),
            stream_name: stream.into(),
        });
    }
}
