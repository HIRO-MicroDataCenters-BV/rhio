#![feature(assert_matches)]

mod actor;
mod config;
mod extensions;
mod logging;
mod message;
mod node;
mod private_key;
mod ticket;

use std::time::Duration;

use anyhow::{Context, Result};
use notify_debouncer_full::notify::{RecursiveMode, Watcher};
use notify_debouncer_full::{new_debouncer, DebounceEventResult, DebouncedEvent};
use p2panda_net::TopicId;
use tokio::sync::mpsc;
use tracing::{error, info};

use crate::config::load_config;
use crate::logging::setup_tracing;
use crate::node::Node;
use crate::private_key::{generate_ephemeral_private_key, generate_or_load_private_key};
use crate::ticket::Ticket;

// @TODO: Use real topic id
const TOPIC_ID: TopicId = [1; 32];

#[tokio::main]
async fn main() -> Result<()> {
    setup_tracing();

    // Load config file and private key
    let config = load_config()?;
    let relay = config.network_config.relay.clone();

    let private_key = match &config.network_config.private_key {
        Some(path) => generate_or_load_private_key(path.clone())
            .context("Could not load private key from file")?,
        None => generate_ephemeral_private_key(),
    };

    // Spawn p2panda node
    let mut node = Node::spawn(config.network_config, private_key.clone()).await?;

    if let Some(addresses) = node.direct_addresses().await {
        match &relay {
            Some(url) => {
                println!("‣ relay url: {}", url);
            }
            None => {
                println!("! you haven't specified a relay address for your node");
                println!("other peers might not be able to connect to you without it.");
            }
        }
        let ticket = Ticket::new(node.id(), addresses, relay);
        println!("‣ connection ticket: {}", ticket);
    } else {
        println!("‣ node public key: {}", node.id());
    }
    println!("‣ watching folder: {}", config.blobs_path.display());
    println!();

    // Watch for changes in the blobs directory
    let (files_tx, mut files_rx) = mpsc::channel::<DebouncedEvent>(1);
    let mut debouncer = new_debouncer(
        Duration::from_secs(2),
        None,
        move |result: DebounceEventResult| match result {
            Ok(events) => {
                for event in events {
                    info!("file added / changed: {event:?}");
                    if let Err(err) = files_tx.blocking_send(event) {
                        error!("failed sending file event: {err}");
                    }
                }
            }
            Err(errors) => {
                for err in errors {
                    error!("error watching file changes: {err}");
                }
            }
        },
    )
    .unwrap();
    debouncer
        .watcher()
        .watch(&config.blobs_path, RecursiveMode::NonRecursive)?;

    // Join p2p gossip overlay and announce blobs from our directory there
    println!("joining gossip overlay ..");

    let _ = node.ready().await;
    println!("gossip overlay joined!");

    loop {
        tokio::select! {
            Some(event) = files_rx.recv() => {
                for path in &event.paths {
                    if let Err(err) = node.import_file(path.clone()).await {
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
