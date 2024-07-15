use std::time::Duration;

use anyhow::Result;
use p2panda_net::network::{InEvent, OutEvent};
use p2panda_net::{LocalDiscovery, NetworkBuilder};
use tracing_subscriber::{prelude::*, EnvFilter};

pub fn setup_logging() {
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_writer(std::io::stderr))
        .with(EnvFilter::from_default_env())
        .try_init()
        .ok();
}

#[tokio::main]
async fn main() -> Result<()> {
    setup_logging();

    let network_id = [0; 32];
    let topic_id = [1; 32];

    let network = NetworkBuilder::new(network_id)
        .discovery(LocalDiscovery::new()?)
        .build()
        .await?;

    let (tx, mut rx) = network.subscribe(topic_id).await?;

    tokio::task::spawn(async move {
        let mut counter: usize = 0;
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            tx.send(InEvent::Message {
                bytes: counter.to_le_bytes().to_vec(),
            })
            .await
            .ok();
            counter += 1;
        }
    });

    tokio::task::spawn(async move {
        while let Ok(event) = rx.recv().await {
            match event {
                OutEvent::Message {
                    bytes,
                    delivered_from,
                } => {
                    println!("{:?} {}", bytes, delivered_from);
                }
            }
        }
    });

    tokio::signal::ctrl_c().await?;

    network.shutdown().await?;

    Ok(())
}
