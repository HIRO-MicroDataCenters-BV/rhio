use anyhow::{Context, Result};
use rhio::config::{load_config, ImportPath};
use rhio::logging::setup_tracing;
use rhio::node::Node;
use rhio::private_key::{generate_ephemeral_private_key, generate_or_load_private_key};
use rhio::ticket::Ticket;
use rhio::topic_id::TopicId;
use rhio::{BLOB_ANNOUNCE_TOPIC, BUCKET_NAME, MINIO_ENDPOINT, MINIO_REGION};
use s3::Region;
use tracing::error;

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

    // Join p2p gossip overlay and announce blobs from our directory there
    println!("joining gossip overlay ..");

    let topic = TopicId::new_from_str(BLOB_ANNOUNCE_TOPIC);
    let (topic_tx, mut topic_rx, ready) = node.subscribe(topic).await?;

    // @TODO: await blob announcement events

    let Some(ImportPath::File(path)) = config.import_path else {
        error!("no import path provided, nothing to do here....");
        return Ok(());
    };

    println!("import blob ..");
    let hash = node.import_blob_filesystem(path.clone()).await?;
    println!("export blob ..");
    node.export_blob_minio(
        hash,
        Region::Custom {
            region: MINIO_REGION.to_string(),
            endpoint: MINIO_ENDPOINT.to_string(),
        },
        BUCKET_NAME.to_string(),
    )
    .await?;
    println!("FINISH");
    node.shutdown().await?;

    Ok(())
}
