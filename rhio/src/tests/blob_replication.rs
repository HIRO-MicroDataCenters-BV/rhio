use crate::{
    tests::configuration::{create_two_node_blob_setup, TwoClusterBlobSetup},
    tracing::setup_tracing,
};
use anyhow::{bail, Context, Result};
use rand::Rng;
use std::time::{Duration, Instant};
use tracing::info;

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
