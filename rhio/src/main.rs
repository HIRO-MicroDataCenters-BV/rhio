use anyhow::{Context, Result};
use rhio::config::load_config;
use rhio::logging::setup_tracing;
use rhio::node::Node;
use rhio::private_key::{generate_ephemeral_private_key, generate_or_load_private_key};
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

    let node: Node<()> = Node::spawn(config.clone(), private_key.clone()).await?;

    if let Some(addresses) = node.direct_addresses().await {
        let addresses: Vec<String> = addresses.iter().map(|addr| addr.to_string()).collect();
        info!("‣ node public key: {}", node.id());
        info!("‣ node addresses: {}", addresses.join(", "));
    }

    // Join gossip overlay for `BLOB_ANNOUNCE_TOPIC` topic
    // println!("joining gossip overlay ..");
    // let topic = TopicId::new_from_str(BLOB_ANNOUNCE_TOPIC);
    // let (topic_tx, mut topic_rx, topic_ready) = node.subscribe(topic).await?;
    //
    // // Wait for the gossip topic to be ready
    // topic_ready.await;
    // println!("gossip overlay joined");

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
