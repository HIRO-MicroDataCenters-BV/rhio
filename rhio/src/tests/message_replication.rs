use std::{collections::HashSet, str::FromStr, sync::Arc, time::Duration};

use crate::{
    nats::client::fake::{
        blocking::BlockingClient,
        client::{test_consumer, FakeNatsClient, FakeNatsMessages},
        server::FakeNatsServer,
    },
    tests::{
        configuration::{
            configure_message_subscription, configure_network, generate_nats_config,
            generate_rhio_config,
        },
        fake_rhio_server::FakeRhioServer,
    },
    tracing::setup_tracing,
};
use anyhow::{Context, Result};
use async_nats::{jetstream::consumer::DeliverPolicy, HeaderMap};
use bytes::Bytes;
use p2panda_core::PrivateKey;
use rhio_config::configuration::NatsConfig;
use rhio_core::Subject;
use tokio::runtime::Builder;
use tracing::info;

#[test]
pub fn test_e2e_message_replication() -> Result<()> {
    setup_tracing(Some("=INFO".into()));

    let TwoClusterMessagingSetup {
        rhio_source,
        rhio_target,
        nats_source,
        nats_target,
        ..
    } = create_two_node_messaging_setup()?;

    // This timeout is quite arbitrary. Ideally, we need to wait till network between peers is established.
    // It seems there is no simple way to learn this at the moment.
    std::thread::sleep(Duration::from_secs(5));
    info!("environment started");

    nats_source.publish("subject".into(), "message".into(), None)?;

    let mut consumer = nats_target.create_consumer(
        test_consumer(),
        "stream".into(),
        vec![Subject::from_str("subject")?],
        DeliverPolicy::All,
    )?;

    let messages = consumer.recv_count(Duration::from_secs(10), 1)?;
    assert_eq!(messages.len(), 1);

    let message = messages.first().unwrap();
    assert_message(
        "message",
        "subject",
        &vec!["X-Rhio-Signature", "X-Rhio-PublicKey"],
        message,
    );

    rhio_source.discard()?;
    rhio_target.discard()?;
    Ok(())
}

#[test]
pub fn test_e2e_resilience_of_message_replication() -> Result<()> {
    setup_tracing(Some("=INFO".into()));

    let TwoClusterMessagingSetup {
        rhio_source,
        rhio_target,
        nats_source,
        nats_target,
        nats_source_config,
        jetstream,
        subject,
        test_runtime,
        ..
    } = create_two_node_messaging_setup()?;

    // This timeout is quite arbitrary. Ideally, we need to wait till network between peers is established.
    // It seems there is no simple way to learn this at the moment.
    std::thread::sleep(Duration::from_secs(5));
    info!("environment started");

    // Sending messages
    for id in 1..=3 {
        nats_source.publish(subject.clone(), format!("message {id}").into(), None)?;
    }

    {
        let mut consumer = nats_target.create_consumer(
            test_consumer(),
            jetstream.clone(),
            vec![Subject::from_str(&subject)?],
            DeliverPolicy::All,
        )?;

        let messages = consumer.recv_count(Duration::from_secs(10), 3)?;
        assert_eq!(messages.len(), 3);
    }

    info!("dropping connections");
    let server =
        FakeNatsServer::get_by_config(&nats_source_config).context("no fake NATS server exists")?;
    server.enable_connection_error();
    let failures = test_runtime
        .block_on(async {
            server
                .wait_for_connection_error(Duration::from_secs(20))
                .await
        })
        .context("no errors happened")?;
    info!("number of failures {}", failures);

    server.disable_connection_error();
    test_runtime
        .block_on(async { server.wait_for_connections(Duration::from_secs(5)).await })
        .context("waiting rhio reconnecting")?;

    std::thread::sleep(Duration::from_secs(3));

    info!("connections restored");
    // Sending messages
    for id in 4..=6 {
        nats_source.publish(subject.clone(), format!("message {id}").into(), None)?;
    }

    // Our fake consumer does not support reconnection, so we need to create a new one
    let mut consumer = nats_target.create_consumer(
        test_consumer(),
        jetstream,
        vec![Subject::from_str(&subject)?],
        DeliverPolicy::All,
    )?;

    let messages = consumer.recv_count(Duration::from_secs(30), 6)?;
    info!("messages {messages:?}");
    assert_eq!(messages.len(), 6);

    rhio_source.discard()?;
    rhio_target.discard()?;
    Ok(())
}

pub fn assert_message(
    expected_payload: &'static str,
    expected_subject: &'static str,
    expected_headers: &Vec<&str>,
    actual: &async_nats::Message,
) {
    let headers = actual
        .headers
        .clone()
        .take()
        .unwrap_or(HeaderMap::default());
    let actual_header_names = headers
        .iter()
        .map(|(name, _)| name.as_ref())
        .collect::<HashSet<&str>>();

    let expected_header_names = expected_headers
        .iter()
        .map(|a| *a)
        .collect::<HashSet<&str>>();

    assert_eq!(expected_header_names, actual_header_names);
    assert_eq!(Bytes::from(expected_payload), actual.payload);
    assert_eq!(
        async_nats::Subject::from_static(expected_subject),
        actual.subject
    );
}

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

    pub(crate) nats_source_config: NatsConfig,
    #[allow(dead_code)]
    pub(crate) nats_target_config: NatsConfig,

    pub(crate) jetstream: String,
    pub(crate) subject: String,

    pub(crate) test_runtime: Arc<tokio::runtime::Runtime>,
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

    let jetstream = String::from("stream");
    let subject = String::from("subject");

    configure_message_subscription(
        &mut rhio_source_config,
        &rhio_source_private_key.public_key(),
        &mut rhio_target_config,
        &jetstream,
        &subject,
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
        test_runtime.clone(),
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
        nats_source_config,
        nats_target_config,
        test_runtime,
        jetstream,
        subject,
    };

    Ok(setup)
}
