use std::io::{Stdin, Write};
use std::str::FromStr;

use anyhow::{Context, Result};
use futures_util::FutureExt;
use p2panda_core::Hash;
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
    let topic_ready = topic_ready.shared();

    // Wait for the gossip topic to be ready
    topic_ready.clone().await;
    println!("gossip overlay joined");

    let node_clone = node.clone();
    pool_handle.spawn_pinned(|| async move {
        let stdin = std::io::stdin();
        while let Ok(input_str) = read_input(&stdin) {
            let import_path = ImportPath::from_str(input_str.trim())?;
            let result = handle_import(&node_clone, import_path).await;

            if let Err(error) = &result {
                tracing::error!("{error}");
                continue;
            }

            // Announce the new blob on the gossip topic so other peers can then download it
            let hash = result.unwrap();
            println!("announce blob: {hash}");
            topic_tx.announce_blob(hash).await?;
        }
        Ok::<_, anyhow::Error>(())
    });

    // For all blob announcements we receive on the gossip topic, download the blob from the
    // network and export it to our own minio store
    while let Ok((message, _)) = topic_rx.recv().await {
        if let Message::BlobAnnouncement(hash) = message {
            node.download_blob(hash).await?;
            node.export_blob_minio(
                hash,
                node.config.bucket_address.region.clone(),
                node.config.bucket_address.endpoint.clone(),
                node.config.bucket_name.clone(),
            )
            .await?;
        }
    }

    tokio::signal::ctrl_c().await?;

    node.shutdown().await?;

    Ok(())
}

async fn handle_import(node: &Node<()>, import_path: ImportPath) -> Result<Hash> {
    let hash = match import_path {
        // Import a file from the local filesystem
        ImportPath::File(path) => {
            println!("import from path: {path:?}");
            node.import_blob_filesystem(path.clone()).await?
        }
        // Import a file from the given url
        ImportPath::Url(url) => {
            println!("import from url: {url}");
            node.import_blob_url(url.clone()).await?
        }
    };

    // Export the blob data from the blob store to a minio bucket
    println!("export to minio bucket: {hash}");
    node.export_blob_minio(
        hash,
        node.config.bucket_address.region.clone(),
        node.config.bucket_address.endpoint.clone(),
        node.config.bucket_name.clone(),
    )
    .await?;

    Ok(hash)
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
