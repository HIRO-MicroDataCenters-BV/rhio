mod actor;
mod config;
mod extensions;
mod logging;
mod message;
mod node;
mod private_key;

use anyhow::{Context, Result};
use notify::{Event, RecursiveMode, Watcher};
use p2panda_net::TopicId;
use tokio::sync::mpsc;
use tracing::{error, info};

use crate::config::load_config;
use crate::logging::setup_tracing;
use crate::node::Node;
use crate::private_key::{generate_ephemeral_private_key, generate_or_load_private_key};

// @TODO: Use real topic id
const TOPIC_ID: TopicId = [1; 32];

#[tokio::main]
async fn main() -> Result<()> {
    setup_tracing();

    // Load config file and private key
    let config = load_config()?;
    let private_key = match &config.network_config.private_key {
        Some(path) => generate_or_load_private_key(path.clone())
            .context("Could not load private key from file")?,
        None => generate_ephemeral_private_key(),
    };

    // Spawn p2panda node
    let mut node = Node::spawn(config.network_config, private_key.clone()).await?;

    if let Some(addresses) = node.direct_addresses().await {
        let values: Vec<String> = addresses.iter().map(|addr| addr.to_string()).collect();
        println!("‣ direct addresses: {}|{}", node.id(), values.join("|"));
    } else {
        println!("‣ node public key: {}", node.id());
    }
    println!("‣ watching folder: {}", config.blobs_path.display());

    // Watch for changes in the blobs directory
    let (files_tx, mut files_rx) = mpsc::channel::<Event>(1);
    let mut watcher = notify::recommended_watcher(move |res| match res {
        Ok(event) => {
            info!("file added / changed: {event:?}");
            if let Err(err) = files_tx.blocking_send(event) {
                error!("failed sending file event: {err}");
            }
        }
        Err(err) => {
            error!("error watching file changes: {err}");
        }
    })?;
    watcher.watch(&config.blobs_path, RecursiveMode::NonRecursive)?;

    // Join p2p gossip overlay and announce blobs from our directory there
    println!("joining gossip overlay ..");

    let _ = node.ready().await;
    println!("gossip overlay joined!");

    loop {
        tokio::select! {
            Some(event) = files_rx.recv() => {
                for path in event.paths {
                    if let Err(err) = node.import_file(path).await {
                        error!("failed announcing new file: {err}");
                    }
                }
            }
            _ = tokio::signal::ctrl_c() => {
                break;
            },
        }
    }

    node.shutdown().await?;

    Ok(())
}
