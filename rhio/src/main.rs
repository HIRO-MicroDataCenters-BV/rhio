use std::io::{Stdin, Write};
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
use tokio_util::task::LocalPoolHandle;

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let pool_handle = LocalPoolHandle::new(num_cpus::get());
    setup_tracing();

    // Load config file and private key
    let config = load_config()?;
    let relay = config.network_config.relay.clone();

    let private_key = match &config.network_config.private_key {
        Some(path) => generate_or_load_private_key(path.clone())
            .context("Could not load private key from file")?,
        None => generate_ephemeral_private_key(),
    };

    let node: Node<()> =
        Node::spawn(config.clone(), private_key.clone(), pool_handle.clone()).await?;

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

    let (tx, mut rx) = tokio::sync::mpsc::channel(10);
    pool_handle.spawn_pinned(|| async move {
        let stdin = std::io::stdin();
        while let Ok(input_str) = read_input(&stdin) {
            let import_path =
                ImportPath::from_str(input_str.trim()).expect("error parsing import string");
            tx.send(import_path).await.expect("channel not closed");
        }
    });

    loop {
        tokio::select! {
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
                println!("received gossip message: {message:?}");
                if let Message::BlobAnnouncement(hash) = message {
                    // @TODO: also abstract away these two methods behind node API
                    node.download_blob(hash).await?;
                    node.export_blob_minio(
                        hash,
                        node.config.minio.region.clone(),
                        node.config.minio.endpoint.clone(),
                        node.config.minio.bucket_name.clone(),
                    )
                    .await?;
                }
            }
            _ = tokio::signal::ctrl_c() => break
        }
    }

    node.shutdown().await?;

    Ok(())
}

fn read_input(stdin: &Stdin) -> Result<String> {
    println!();
    print!("Enter file path or URL: ");
    let _ = std::io::stdout().flush();
    let mut buffer = String::new();
    stdin.read_line(&mut buffer)?;
    println!();
    Ok(buffer)
}
