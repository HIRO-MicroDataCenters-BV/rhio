use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Context, Result};
use notify_debouncer_full::notify::{EventKind, RecursiveMode, Watcher};
use notify_debouncer_full::{new_debouncer, DebounceEventResult};
use rhio::config::load_config;
use rhio::logging::setup_tracing;
use rhio::messages::Message;
use rhio::node::Node;
use rhio::private_key::{generate_ephemeral_private_key, generate_or_load_private_key};
use rhio::topic_id::TopicId;
use rhio::FILE_SYSTEM_EVENT_TOPIC;
use tokio::sync::mpsc;
use tracing::{error, info};

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

    let mut node = Node::spawn(config.clone(), private_key.clone()).await?;

    if let Some(addresses) = node.direct_addresses().await {
        let values: Vec<String> = addresses.iter().map(|addr| addr.to_string()).collect();
        println!("‣ direct addresses: {}|{}", node.id(), values.join("|"));
    } else {
        println!("‣ node public key: {}", node.id());
    }
    println!("‣ watching folder: {}", config.blobs_path.display());
    println!();

    // Watch for changes in the blobs directory
    let (files_tx, mut files_rx) = mpsc::channel::<Vec<PathBuf>>(1);
    let mut debouncer = new_debouncer(
        Duration::from_secs(2),
        None,
        move |result: DebounceEventResult| match result {
            Ok(events) => {
                for event in events {
                    match event.kind {
                        EventKind::Create(_) => (),
                        _ => continue, // ignore all other events
                    }

                    info!("file added / changed: {event:?}");
                    if let Err(err) = files_tx.blocking_send(event.paths.clone()) {
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

    // Get tx and rx for sending and receiving events on the FILE_SYSTEM_EVENT_TOPIC gossip
    // topic.
    //
    // @TODO: Support configuring the network with custom application topics
    let (fs_tx, mut fs_rx) = node
        .subscribe::<Message>(TopicId::from_str(FILE_SYSTEM_EVENT_TOPIC))
        .await?;

    // Listen for arriving events.
    tokio::spawn(async move {
        while let Ok(msg) = fs_rx.recv().await {
            println!("{msg:?}")
        }
    });

    // Send "hello" on the channel.
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            let _ = fs_tx
                .send(Message::Application("hello".as_bytes().to_vec()))
                .await;
        }
    });

    loop {
        tokio::select! {
            Some(paths) = files_rx.recv() => {
                for path in paths {
                    if let Err(err) = node.sync_file(path.clone()).await {
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
