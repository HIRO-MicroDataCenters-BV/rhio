use anyhow::Result;
use iroh_net::endpoint::DirectAddr;
use iroh_net::NodeId;
use p2panda_blobs::{Blobs, MemoryStore};
use p2panda_core::PrivateKey;
use p2panda_net::config::Config;
use p2panda_net::{LocalDiscovery, Network, NetworkBuilder};

pub struct Node {
    config: Config,
    network: Network,
    blobs: Blobs<MemoryStore>,
}

impl Node {
    pub async fn spawn(config: Config, private_key: PrivateKey) -> Result<Self> {
        let store = MemoryStore::new();

        let network_builder = NetworkBuilder::from_config(config.clone())
            .private_key(private_key)
            .discovery(LocalDiscovery::new()?);

        let (network, blobs) = Blobs::from_builder(network_builder, store).await?;

        let node = Node {
            config,
            network,
            blobs,
        };

        Ok(node)
    }

    pub async fn direct_addresses(&self) -> Option<Vec<DirectAddr>> {
        self.network.direct_addresses().await
    }

    pub fn node_id(&self) -> NodeId {
        self.network.node_id()
    }

    pub async fn shutdown(self) -> Result<()> {
        // Trigger shutdown of the main run task by activating the cancel token
        self.network.shutdown().await?;
        Ok(())
    }
}
