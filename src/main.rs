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
    let node = Node::spawn(config).await?;
    info!("Node ID: {}", node.node_id());

    tokio::select! {
        _ = tokio::signal::ctrl_c() => (),
    }

    node.shutdown().await?;

    Ok(())
}
