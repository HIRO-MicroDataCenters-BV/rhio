use anyhow::{Context, Result};
use rhio::tracing::setup_tracing;
use rhio::{config::load_config, Subscription};
use rhio::{Node, Publication};
use rhio_core::{
    generate_ephemeral_private_key, generate_or_load_private_key, ScopedBucket, ScopedSubject,
};
use tracing::info;

#[tokio::main]
async fn main() -> Result<()> {
    let config = load_config()?;

    setup_tracing(config.log_level.clone());

    let private_key = match &config.node.private_key {
        Some(path) => generate_or_load_private_key(path.clone())
            .context("Could not load private key from file")?,
        None => generate_ephemeral_private_key(),
    };

    let node = Node::spawn(config.clone(), private_key.clone()).await?;

    hello_rhio();
    let addresses: Vec<String> = node
        .direct_addresses()
        .iter()
        .map(|addr| addr.to_string())
        .collect();
    info!("‣ network id:");
    info!("  - {}", config.node.network_id);
    info!("‣ node public key:");
    info!("  - {}", node.id());
    info!("‣ node addresses:");
    for address in addresses {
        info!("  - {}", address);
    }

    // For data we're publishing we subscribe to the same data stream as all other peers. On this
    // level we don't distinct if the data is ours or someone else's, it only counts that we can
    // support the network with it and that we have an interest in it.
    if let Some(publish) = config.publish {
        for bucket in publish.s3_buckets {
            node.subscribe(Subscription::Bucket(ScopedBucket::new(node.id(), &bucket)))
                .await?;
        }

        for subject in publish.nats_subjects {
            node.subscribe(Subscription::Subject(ScopedSubject::new(
                node.id(),
                &subject,
            )))
            .await?;
        }
    };

    if let Some(subscribe) = config.subscribe {
        for bucket in subscribe.s3_buckets {
            node.subscribe(Subscription::Bucket(bucket)).await?;
        }

        for subject in subscribe.nats_subjects {
            node.subscribe(Subscription::Subject(subject)).await?;
        }
    };

    tokio::signal::ctrl_c().await?;

    info!("");
    info!("shutting down");
    node.shutdown().await?;

    Ok(())
}

fn hello_rhio() {
    r#"      ___           ___                       ___
     /\  \         /\__\          ___        /\  \
    /::\  \       /:/  /         /\  \      /::\  \
   /:/\:\  \     /:/__/          \:\  \    /:/\:\  \
  /::\~\:\  \   /::\  \ ___      /::\__\  /:/  \:\  \
 /:/\:\ \:\__\ /:/\:\  /\__\  __/:/\/__/ /:/__/ \:\__\
 \/_|::\/:/  / \/__\:\/:/  / /\/:/  /    \:\  \ /:/  /
    |:|::/  /       \::/  /  \::/__/      \:\  /:/  /
    |:|\/__/        /:/  /    \:\__\       \:\/:/  /
    |:|  |         /:/  /      \/__/        \::/  /
     \|__|         \/__/                     \/__/ 
    "#
    .split("\n")
    .for_each(|line| info!("{}", line));
}
