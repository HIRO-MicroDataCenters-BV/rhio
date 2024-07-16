mod config;
mod logging;
mod node;
mod private_key;

use anyhow::{Context, Result};
use tracing::info;

use crate::config::load_config;
use crate::logging::setup_tracing;
use crate::node::Node;
use crate::private_key::{generate_ephemeral_private_key, generate_or_load_private_key};

#[tokio::main]
async fn main() -> Result<()> {
    setup_tracing();
    let config = load_config()?;

    let private_key = match &config.private_key {
        Some(path) => generate_or_load_private_key(path.clone())
            .context("Could not load private key from file")?,
        None => generate_ephemeral_private_key(),
    };
    info!("My public key: {}", private_key.public_key());

    let node = Node::spawn(config, private_key).await?;

    // Upload blob
    // let mut stream = node.add_blob("/home/adz/website.html".into()).await;
    // while let Some(item) = stream.next().await {
    //     println!("{:?}", item);
    // }

    // Download blob
    // let hash = "1eafd71f60630c8826fbc7de90bbe046b956f3d9397ee5c0fd48f24bc80c0e31"
    //     .parse()
    //     .unwrap();
    // let mut stream = node.blob_download(hash).await;
    // while let Some(item) = stream.next().await {
    //     println!("{:?}", item);
    // }

    tokio::signal::ctrl_c().await?;

    node.shutdown().await?;

    Ok(())
}
