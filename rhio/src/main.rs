mod config;
mod extensions;
mod logging;
mod message;
mod node;
mod operations;
mod private_key;

use std::time::Duration;

use anyhow::{Context, Result};
use p2panda_core::Hash;
use p2panda_net::TopicId;
use tracing::info;

use crate::config::load_config;
use crate::logging::setup_tracing;
use crate::message::Message;
use crate::node::Node;
use crate::private_key::{generate_ephemeral_private_key, generate_or_load_private_key};

// @TODO: Use real topic id
const TOPIC_ID: TopicId = [1; 32];

#[tokio::main]
async fn main() -> Result<()> {
    setup_tracing();
    let config = load_config()?;

    let private_key = match &config.private_key {
        Some(path) => generate_or_load_private_key(path.clone())
            .context("Could not load private key from file")?,
        None => generate_ephemeral_private_key(),
    };
    info!("My public key: {}", private_key.public_key());

    let mut node = Node::spawn(config, private_key.clone()).await?;

    // Upload blob
    // let mut stream = node
    //     .import_blob("/home/adz/downloads/1ec8d4986b04fd80.png".into())
    //     .await;
    // while let Some(item) = stream.next().await {
    //     println!("{:?}", item);
    // }

    // Wait until we've discovered other nodes
    // tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    // Download blob
    // let hash = "874be4e87da990b66cba5c964dfa50d720acc97a4c133e28453d240976080eb8"
    //     .parse()
    //     .unwrap();
    // let mut stream = node.download_blob(hash).await;
    // while let Some(item) = stream.next().await {
    //     println!("{:?}", item);
    // }

    println!("Node ID: {}", node.node_id());
    println!("joining gossip overlay...");

    let _ = node.ready().await;
    println!("gossip overlay joined!");

    let mut interval = tokio::time::interval(Duration::from_secs(1));

    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                node.shutdown().await?;
                return Ok(());
            },
            _ = interval.tick() => {
                node.send_message(Message::AnnounceBlob(Hash::new(vec![1, 2, 3]))).await?;
            }
        }
    }
}
