use anyhow::{Context, Result};
use futures_util::FutureExt;
use rhio::config::{load_config, ImportPath};
use rhio::logging::setup_tracing;
use rhio::messages::Message;
use rhio::node::Node;
use rhio::private_key::{generate_ephemeral_private_key, generate_or_load_private_key};
use rhio::ticket::Ticket;
use rhio::topic_id::TopicId;
use rhio::{BLOB_ANNOUNCE_TOPIC, BUCKET_NAME, MINIO_ENDPOINT, MINIO_REGION};
use tokio_util::task::LocalPoolHandle;
use tracing::info;

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
    info!("joining gossip overlay ..");
    let topic = TopicId::new_from_str(BLOB_ANNOUNCE_TOPIC);
    let (topic_tx, mut topic_rx, topic_ready) = node.subscribe(topic).await?;
    let topic_ready = topic_ready.shared();

    // Wait for the gossip topic to be ready
    topic_ready.clone().await;
    info!("gossip overlay joined");

    // Spawn a separate task to import a file, export it to minio and then announce it on the network.
    let node_clone = node.clone();

    pool_handle.spawn_pinned(|| async move {
        let hash = match &config.import_path {
            Some(location) => match location {
                // Import a file from the local filesystem
                ImportPath::File(path) => {
                    info!("import {path:?}");
                    let hash = node_clone
                        .import_blob_filesystem(path.clone())
                        .await
                        .expect("import blob from filesystem failed");
                    hash
                }
                // Import a file from the given url
                ImportPath::Url(url) => {
                    info!("import {url}");
                    let hash = node_clone
                        .import_blob_url(url.clone())
                        .await
                        .expect("import blob from url failed");
                    hash
                }
            },
            None => return,
        };

        // Export the blob data from the blob store to a minio bucket
        info!("export to minio {hash}");
        node_clone
            .export_blob_minio(
                hash,
                MINIO_REGION.to_string(),
                MINIO_ENDPOINT.to_string(),
                BUCKET_NAME.to_string(),
            )
            .await
            .expect("export blob failed");

        // Announce the new blob on the gossip topic so other peers can then download it
        info!("announce blob {hash}");
        topic_tx
            .announce_blob(hash)
            .await
            .expect("failed to send message on topic");
    });

    // For all blob announcements we receive on the gossip topic, download the blob from the
    // network and export it to our own minio store
    while let Ok((message, _)) = topic_rx.recv().await {
        match message {
            Message::BlobAnnouncement(hash) => {
                info!("download {hash}");
                node.download_blob(hash).await?;
                info!("export to minio {hash}");
                node.export_blob_minio(
                    hash,
                    MINIO_REGION.to_string(),
                    MINIO_ENDPOINT.to_string(),
                    BUCKET_NAME.to_string(),
                )
                .await?;
            }
            _ => (),
        }
    }

    node.shutdown().await?;

    Ok(())
}
