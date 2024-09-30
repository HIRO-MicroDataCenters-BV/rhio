use std::str::FromStr;

use anyhow::{Context, Result};
use rhio::config::load_config;
use rhio::tracing::setup_tracing;
use rhio::Node;
use rhio_core::{
    generate_ephemeral_private_key, generate_or_load_private_key, log_id::RhioTopicMap, LogId,
    TopicId,
};
use tracing::{info, warn};

#[tokio::main]
async fn main() -> Result<()> {
    let config = load_config()?;

    setup_tracing(config.log_level.clone());
    hello_rhio();

    let private_key = match &config.node.private_key {
        Some(path) => generate_or_load_private_key(path.clone())
            .context("Could not load private key from file")?,
        None => generate_ephemeral_private_key(),
    };

    let mut topic_map = RhioTopicMap::default();
    config
        .streams
        .clone()
        .unwrap_or_default()
        .iter()
        .for_each(|stream_config| {
            let Some(subject) = &stream_config.nats_filter_subject else {
                warn!(
                    "no subject provided, not subscribing to {}",
                    stream_config.external_topic
                );
                return;
            };

            let log_id = LogId::new(subject);

            topic_map.insert(
                TopicId::from_str(&stream_config.external_topic)
                    .unwrap()
                    .into(),
                log_id,
            );
        });

    let node = Node::spawn(config.clone(), private_key.clone(), topic_map).await?;

    let addresses: Vec<String> = node
        .direct_addresses()
        .iter()
        .map(|addr| addr.to_string())
        .collect();
    info!("‣ node public key:");
    info!("  - {}", node.id());
    info!("‣ node addresses:");
    for address in addresses {
        info!("  - {}", address);
    }

    match config.streams {
        Some(streams) => {
            info!("‣ streams:");
            streams.iter().for_each(|stream| {
                info!(
                    "  - \"{}\"{} [internal] <-> \"{}\" [external]",
                    stream.nats_stream_name,
                    stream
                        .nats_filter_subject
                        .clone()
                        .map_or_else(|| "".into(), |value| format!(" (\"{value}\")")),
                    stream.external_topic,
                )
            });

            for stream in streams {
                node.subscribe(
                    stream.nats_stream_name,
                    stream.nats_filter_subject,
                    TopicId::from_str(&stream.external_topic)?,
                )
                .await?;
            }
        }
        None => {
            warn!("no streams defined in config file to subscribe to!");
        }
    }

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
