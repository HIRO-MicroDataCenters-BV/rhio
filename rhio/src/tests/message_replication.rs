use std::{collections::HashSet, str::FromStr, time::Duration};

use crate::{
    tests::configuration::{create_two_node_messaging_setup, TwoClusterMessagingSetup},
    tracing::setup_tracing,
};
use anyhow::Result;
use async_nats::{jetstream::consumer::DeliverPolicy, HeaderMap};
use bytes::Bytes;
use rhio_core::Subject;
use tracing::info;

#[test]
pub fn test_e2e_message_replication() -> Result<()> {
    setup_tracing(Some("=INFO".into()));

    let TwoClusterMessagingSetup {
        rhio_source,
        rhio_target,
        nats_source,
        nats_target,
    } = create_two_node_messaging_setup()?;

    // This timeout is quite arbitrary. Ideally, we need to wait till network between peers is established.
    // It seems there is no simple way to learn this at the moment.
    std::thread::sleep(Duration::from_secs(5));
    info!("environment started");

    nats_source.publish("test.subject1".into(), "test message".into(), None)?;

    let mut consumer = nats_target.create_consumer(
        "test-stream".into(),
        vec![Subject::from_str("test.subject1")?],
        DeliverPolicy::All,
    )?;

    let messages = consumer.recv_count(Duration::from_secs(10), 1)?;
    assert_eq!(messages.len(), 1);

    let message = messages.first().unwrap();
    assert_message(
        "test message",
        "test.subject1",
        &vec!["X-Rhio-Signature", "X-Rhio-PublicKey"],
        message,
    );

    rhio_source.discard()?;
    rhio_target.discard()?;
    Ok(())
}

fn assert_message(
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
