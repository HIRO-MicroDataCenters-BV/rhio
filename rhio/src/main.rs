use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result};
use notify_debouncer_full::notify::{EventKind, RecursiveMode, Watcher};
use notify_debouncer_full::{new_debouncer, DebounceEventResult};
use rhio::aggregate::{FileSystem, FileSystemAction};
use rhio::config::load_config;
use rhio::logging::setup_tracing;
use rhio::messages::{FileSystemEvent, Message};
use rhio::node::Node;
use rhio::private_key::{generate_ephemeral_private_key, generate_or_load_private_key};
use rhio::ticket::Ticket;
use rhio::topic_id::TopicId;
use rhio::FILE_SYSTEM_EVENT_TOPIC;
use tokio::sync::mpsc;
use tracing::{error, info};

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

    let node: Node<()> = Node::spawn(config.network_config.clone(), private_key.clone()).await?;

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

    // Join p2p gossip overlay and announce blobs from our directory there
    println!("joining gossip overlay ..");

    let fs_topic = TopicId::new_from_str(FILE_SYSTEM_EVENT_TOPIC);
    let (fs_topic_tx, mut fs_topic_rx, ready) = node.subscribe(fs_topic).await?;
    ready.await;

    println!("gossip overlay joined!");

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

    let mut file_system = FileSystem::new();
    let mut exported_blobs = HashSet::new();

    loop {
        tokio::select! {
            Some(paths) = files_rx.recv() => {
                for path in paths {
                    let relative_path = to_relative_path(&path, &config.blobs_path);
                    if !exported_blobs.remove(&relative_path) {
                        info!("file added: {path:?}");
                        let hash = node.import_blob(path.clone()).await.expect("can import blob");
                        let fs_event = FileSystemEvent::Create(relative_path, hash);
                        let context = fs_topic_tx.send(Message::FileSystem(fs_event.clone())).await.expect("can send topic event");
                        let _ = file_system.process(fs_event, context.operation_timestamp);
                    }
                }
            }
            Ok((message, context)) = fs_topic_rx.recv() => {
                match message {
                    Message::FileSystem(event) => {
                        let actions = file_system.process(event, context.operation_timestamp);
                        for action in actions {
                            match action {
                                FileSystemAction::DownloadAndExport { hash, path } => {
                                    if node.download_blob(hash).await.is_ok() {
                                        node.export_blob(hash, config.blobs_path.join(&path)).await.expect("failed to export blob");
                                        exported_blobs.insert(path);
                                    }
                                }
                                FileSystemAction::Export { hash, path } => {
                                        node.export_blob(hash, config.blobs_path.join(&path)).await.expect("failed to export blob");
                                        exported_blobs.insert(path);
                                }
                            }
                        }
                    },
                    _ => panic!("received unexpected message")
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

fn to_relative_path(path: &Path, base: &Path) -> PathBuf {
    path.strip_prefix(base)
        .expect("Blob import path contains blob dir")
        .to_path_buf()
}
