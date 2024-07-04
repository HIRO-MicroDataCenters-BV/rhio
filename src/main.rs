#![allow(dead_code)]
mod config;
mod logging;
mod node;
mod protocol;

use anyhow::Result;
use tracing::info;

use crate::config::load_config;
use crate::logging::setup_tracing;
use crate::node::Node;

#[tokio::main]
async fn main() -> Result<()> {
    setup_tracing();

    let config = load_config()?;
    let node = Node::spawn(config.clone()).await?;

    if let Some(addresses) = node.direct_addresses().await {
        let values: Vec<String> = addresses.iter().map(|item| item.addr.to_string()).collect();
        info!("Direct addresses: {}|{}", node.node_id(), values.join("|"));
    } else {
        info!("Node ID: {}", node.node_id());
    }

    for node_addr in config.direct_node_addresses {
        let connection = node.connect(node_addr).await?;
        let mut stream = connection.accept_uni().await?;
        let bytes = stream.read_to_end(32).await?;
        info!("received data: {:?}", bytes);
    }

    tokio::select! {
        _ = tokio::signal::ctrl_c() => (),
    }

    node.shutdown().await?;

    Ok(())
}
