use anyhow::{Context, Result};
use async_nats::Subject;
use rhio::config::load_config;
use rhio::logging::setup_tracing;
use rhio::node::Node;
use rhio::private_key::{generate_ephemeral_private_key, generate_or_load_private_key};
use tracing::{error, info};

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

    let node: Node<()> = Node::spawn(config.clone(), private_key.clone()).await?;

    if let Some(addresses) = node.direct_addresses().await {
        let addresses: Vec<String> = addresses.iter().map(|addr| addr.to_string()).collect();
        info!("‣ node public key: {}", node.id());
        info!("‣ node addresses: {}", addresses.join(", "));
    }

    // @TODO: Subscribe to streams based on config file instead
    info!("subscribe to NATS stream");
    let initial_download_ready = node
        .subscribe("my_stream".into(), Subject::from_static("foo"))
        .await?;
    if let Err(err) = initial_download_ready.await? {
        error!("initial download failed: {err}");
    } else {
        info!("initial download ready");
    }

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
