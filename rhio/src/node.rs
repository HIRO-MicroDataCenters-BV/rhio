use std::path::PathBuf;

use anyhow::Result;
use futures_util::Stream;
use iroh_net::endpoint::DirectAddr;
use iroh_net::NodeId;
use p2panda_blobs::{Blobs, DownloadBlobEvent, ImportBlobEvent, MemoryStore};
use p2panda_core::{Hash, PrivateKey};
use p2panda_net::config::Config;
use p2panda_net::{LocalDiscovery, Network, NetworkBuilder};

pub struct Node {
    #[allow(dead_code)]
    config: Config,
    network: Network,
    #[allow(dead_code)]
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

    #[allow(dead_code)]
    pub async fn direct_addresses(&self) -> Option<Vec<DirectAddr>> {
        self.network.direct_addresses().await
    }

    #[allow(dead_code)]
    pub fn node_id(&self) -> NodeId {
        self.network.node_id()
    }

    pub async fn import_blob(&self, path: PathBuf) -> impl Stream<Item = ImportBlobEvent> {
        self.blobs.import_blob(path).await
    }

    pub async fn download_blob(&self, hash: Hash) -> impl Stream<Item = DownloadBlobEvent> {
        self.blobs.download_blob(hash).await
    }

    pub async fn shutdown(self) -> Result<()> {
        // Trigger shutdown of the main run task by activating the cancel token
        self.network.shutdown().await?;
        Ok(())
    }
}
