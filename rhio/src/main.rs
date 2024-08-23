use std::io::Write;
use std::str::FromStr;

use anyhow::{Context, Result};
use rhio::config::{load_config, ImportPath};
use rhio::logging::setup_tracing;
use rhio::messages::Message;
use rhio::node::Node;
use rhio::private_key::{generate_ephemeral_private_key, generate_or_load_private_key};
use rhio::ticket::Ticket;
use rhio::topic_id::TopicId;
use rhio::BLOB_ANNOUNCE_TOPIC;
use tokio::sync::mpsc;

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

    let node: Node<()> = Node::spawn(config.clone(), private_key.clone()).await?;

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

    // Join gossip overlay for `BLOB_ANNOUNCE_TOPIC` topic
    println!("joining gossip overlay ..");
    let topic = TopicId::new_from_str(BLOB_ANNOUNCE_TOPIC);
    let (topic_tx, mut topic_rx, topic_ready) = node.subscribe(topic).await?;

    // Wait for the gossip topic to be ready
    topic_ready.await;
    println!("gossip overlay joined");

    // Spawn dedicated thread for accepting user input
    let (tx, mut rx) = tokio::sync::mpsc::channel(1);
    std::thread::spawn(move || input_loop(tx));

    loop {
        tokio::select! {
            biased;

            _ = tokio::signal::ctrl_c() => break,
            // Handle import path inputs arriving from stdin
            Some(import_path) = rx.recv() => {
                let result = node.import_blob(import_path).await;
                match result {
                    Ok(hash) => {
                        let _ = topic_tx.announce_blob(hash).await?;
                    },
                    Err(err) => {
                        tracing::error!("{err}");
                    },
                }
            },
            // For all blob announcements we receive on the gossip topic, download the blob from the
            // network and export it to our own minio store
            Ok((message, _)) = topic_rx.recv() => {
                if let Message::BlobAnnouncement(hash) = message {
                    node.download_blob(hash).await?;
                }
            }
        }
    }

    println!();
    println!("shutting down");
    node.shutdown().await?;

    Ok(())
}

fn input_loop(line_tx: mpsc::Sender<ImportPath>) -> Result<()> {
    let mut buffer = String::new();
    let stdin = std::io::stdin();
    loop {
        println!();
        print!("Enter file path or URL: ");
        let _ = std::io::stdout().flush();
        stdin.read_line(&mut buffer)?;
        println!();
        let import_path = ImportPath::from_str(buffer.trim()).expect("error parsing import string");
        line_tx.blocking_send(import_path)?;
        buffer.clear();
    }
}
