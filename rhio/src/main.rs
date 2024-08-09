use anyhow::{Context, Result};
use minio::s3::args::{BucketExistsArgs, MakeBucketArgs};
use minio::s3::builders::ObjectContent;
use minio::s3::client::ClientBuilder;
use minio::s3::creds::StaticProvider;
use minio::s3::http::BaseUrl;
use rhio::config::load_config;
use rhio::logging::setup_tracing;
use rhio::node::Node;
use rhio::private_key::{generate_ephemeral_private_key, generate_or_load_private_key};
use rhio::ticket::Ticket;
use rhio::topic_id::TopicId;
use rhio::{BLOB_ANNOUNCE_TOPIC, BUCKET_NAME, MINIO_ADDRESS};
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

    // Join p2p gossip overlay and announce blobs from our directory there
    println!("joining gossip overlay ..");

    let topic = TopicId::new_from_str(BLOB_ANNOUNCE_TOPIC);
    let (topic_tx, mut topic_rx, ready) = node.subscribe(topic).await?;

    let base_url = MINIO_ADDRESS.parse::<BaseUrl>()?;

    info!("Trying to connect to MinIO at: `{:?}`", base_url);

    let Some(credentials) = config.minio_credentials else {
        error!("minio access credentials required but not provided");
        return Ok(());
    };

    let static_provider =
        StaticProvider::new(&credentials.access_key, &credentials.secret_key, None);

    let client = ClientBuilder::new(base_url.clone())
        .provider(Some(Box::new(static_provider)))
        .build()?;

    // Check 'bucket_name' bucket exist or not.
    let exists: bool = client
        .bucket_exists(&BucketExistsArgs::new(BUCKET_NAME).unwrap())
        .await
        .unwrap();

    // Make 'bucket_name' bucket if not exist.
    if !exists {
        client
            .make_bucket(&MakeBucketArgs::new(BUCKET_NAME).unwrap())
            .await
            .unwrap();
    }

    let Some(import_path) = config.import_path else {
        error!("no import path provided, nothing to do here....");
        return Ok(());
    };

    let filename = import_path
        .file_name()
        .expect("import path ends with valid filename")
        .to_str()
        .expect("filename is valid UTF-8 string");

    // Name of the object that will be stored in the bucket
    let object_name: &str = filename;

    info!("import: {}", &import_path.to_str().unwrap());

    let content = ObjectContent::from(filename.to_string());
    client
        .put_object_content(BUCKET_NAME, object_name, content)
        .send()
        .await?;

    info!(
        "file `{}` is successfully uploaded as object `{object_name}` to bucket `{BUCKET_NAME}`.",
        filename
    );

    node.shutdown().await?;

    Ok(())
}
