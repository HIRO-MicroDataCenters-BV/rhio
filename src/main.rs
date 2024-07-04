#![allow(dead_code)]
mod blobs;
mod config;
mod logging;
mod node;
mod protocol;

use anyhow::Result;
use futures_lite::StreamExt;
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
        info!(
            "My direct addresses: {}|{}",
            node.node_id(),
            values.join("|")
        );
    } else {
        info!("My Node ID: {}", node.node_id());
    }

    // Upload blob
    // let mut stream = node.add_blob("/home/adz/website.html".into()).await;
    // while let Some(item) = stream.next().await {
    //     println!("{:?}", item);
    // }

    // Download blob
    // let hash = "2c8cb6cd9d8ad329f0af2de958df1bd461677cff83a087757d196424d9118bc8".parse().unwrap();
    // let mut stream = node.blob_download(hash).await;
    // while let Some(item) = stream.next().await {
    //     println!("{:?}", item);
    // }

    tokio::select! {
        _ = tokio::signal::ctrl_c() => (),
    }

    node.shutdown().await?;

    Ok(())
}
