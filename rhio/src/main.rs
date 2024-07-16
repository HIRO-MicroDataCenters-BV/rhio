mod config;
mod logging;
mod node;
mod private_key;

use anyhow::{Context, Result};
use futures_util::StreamExt;
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
    // let mut stream = node
    //     .import_blob("/home/adz/downloads/1ec8d4986b04fd80.png".into())
    //     .await;
    // while let Some(item) = stream.next().await {
    //     println!("{:?}", item);
    // }

    // Wait until we've discovered other nodes
    tokio::time::sleep(std::time::Duration::from_secs(5)).await;

    // Download blob
    let hash = "874be4e87da990b66cba5c964dfa50d720acc97a4c133e28453d240976080eb8"
        .parse()
        .unwrap();
    let mut stream = node.download_blob(hash).await;
    while let Some(item) = stream.next().await {
        println!("{:?}", item);
    }

    tokio::signal::ctrl_c().await?;

    node.shutdown().await?;

    Ok(())
}
