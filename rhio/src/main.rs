use anyhow::{Context, Result};
use rhio::config::load_config;
use rhio::tracing::setup_tracing;
use rhio::Node;
use rhio_core::{generate_ephemeral_private_key, generate_or_load_private_key};
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    let config = load_config()?;

    setup_tracing(config.log_level.clone());
    hello_rhio();

    let private_key = match &config.node.private_key {
        Some(path) => generate_or_load_private_key(path.clone())
            .context("Could not load private key from file")?,
        None => generate_ephemeral_private_key(),
    };

    let node = Node::spawn(config.clone(), private_key.clone()).await?;

    let addresses: Vec<String> = node
        .direct_addresses()
        .iter()
        .map(|addr| addr.to_string())
        .collect();
    info!("‣ node public key: {}", node.id());
    info!("‣ node addresses: {}", addresses.join(", "));

    // @TODO: Subscribe to streams based on config file instead
    info!("subscribe to NATS stream");
    node.subscribe("my_test".into(), Some("test.*".into()))
        .await?;

    tokio::signal::ctrl_c().await?;

    info!("");
    info!("shutting down");
    node.shutdown().await?;

    Ok(())
}

fn hello_rhio() {
    r#"      ___           ___                       ___
     /\  \         /\__\          ___        /\  \
    /::\  \       /:/  /         /\  \      /::\  \
   /:/\:\  \     /:/__/          \:\  \    /:/\:\  \
  /::\~\:\  \   /::\  \ ___      /::\__\  /:/  \:\  \
 /:/\:\ \:\__\ /:/\:\  /\__\  __/:/\/__/ /:/__/ \:\__\
 \/_|::\/:/  / \/__\:\/:/  / /\/:/  /    \:\  \ /:/  /
    |:|::/  /       \::/  /  \::/__/      \:\  /:/  /
    |:|\/__/        /:/  /    \:\__\       \:\/:/  /
    |:|  |         /:/  /      \/__/        \::/  /
     \|__|         \/__/                     \/__/ 
    "#
    .split("\n")
    .for_each(|line| info!("{}", line));
}
