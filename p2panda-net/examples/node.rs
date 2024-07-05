use anyhow::Result;
use p2panda_net::{MulticastDNSDiscovery, NetworkBuilder};
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

    let mdns = MulticastDNSDiscovery::new(network_id);
    let network = NetworkBuilder::new(network_id)
        .discovery(mdns)
        .build()
        .await?;

    network.subscribe(topic_id).await?;

    tokio::select! {
        _ = tokio::signal::ctrl_c() => (),
    }

    network.shutdown().await?;

    Ok(())
}
