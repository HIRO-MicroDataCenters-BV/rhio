#![allow(unused)]
mod blobs;
mod config;
mod logging;
mod node;
mod protocol;

use std::str::FromStr;

use anyhow::Result;
use futures_lite::StreamExt;
use iroh_base::node_addr::AddrInfoOptions;
use iroh_net::defaults::staging::EU_RELAY_HOSTNAME;
use iroh_net::relay::{RelayMode, RelayUrl};
use p2panda_net::config::to_node_addr;
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

    for (public_key, addresses) in config.direct_node_addresses.iter() {
        // let mut node_addr = node_addr.clone();
        // node_addr.apply_options(AddrInfoOptions::Id);
        // node_addr = node_addr.with_relay_url(RelayUrl::from_str(EU_RELAY_HOSTNAME).unwrap());
        let connection = node.connect(to_node_addr(public_key, addresses)).await?;
        let mut stream = connection.accept_uni().await?;
        let bytes = stream.read_to_end(32).await?;
        info!("{:?}", bytes);
    }

    // Upload blob
    // let mut stream = node.add_blob("/home/adz/website.html".into()).await;
    // while let Some(item) = stream.next().await {
    //     println!("{:?}", item);
    // }

    // Download blob
    // let hash = "1eafd71f60630c8826fbc7de90bbe046b956f3d9397ee5c0fd48f24bc80c0e31"
    //     .parse()
    //     .unwrap();
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
