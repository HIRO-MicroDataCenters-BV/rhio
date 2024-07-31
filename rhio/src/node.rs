use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::Result;
use p2panda_blobs::{Blobs, MemoryStore as BlobMemoryStore};
use p2panda_core::{PrivateKey, PublicKey};
use p2panda_net::config::Config;
use p2panda_net::{LocalDiscovery, Network, NetworkBuilder};
use p2panda_store::MemoryStore as LogMemoryStore;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::actor::{RhioActor, ToRhioActor};
use crate::TOPIC_ID;

pub struct Node {
    network: Network,
    rhio_actor_tx: mpsc::Sender<ToRhioActor>,
    actor_handle: JoinHandle<()>,
    ready_rx: mpsc::Receiver<()>,
}

impl Node {
    pub async fn spawn(config: Config, private_key: PrivateKey) -> Result<Self> {
        let (rhio_actor_tx, rhio_actor_rx) = mpsc::channel(256);
        let (ready_tx, ready_rx) = mpsc::channel::<()>(1);

        let blob_store = BlobMemoryStore::new();
        let log_store = LogMemoryStore::default();

        let network_builder = NetworkBuilder::from_config(config.clone())
            .private_key(private_key.clone())
            .discovery(LocalDiscovery::new()?);

        let (network, blobs) = Blobs::from_builder(network_builder, blob_store).await?;
        let (topic_tx, topic_rx) = network.subscribe(TOPIC_ID).await?;

        let mut rhio_actor = RhioActor::new(
            blobs.clone(),
            private_key.clone(),
            log_store,
            topic_tx,
            topic_rx,
            rhio_actor_rx,
            ready_tx,
        );

        let actor_handle = tokio::task::spawn(async move {
            if let Err(err) = rhio_actor.run().await {
                panic!("operations actor failed: {err:?}");
            }
        });

        let node = Node {
            network,
            rhio_actor_tx,
            actor_handle,
            ready_rx,
        };

        Ok(node)
    }

    pub async fn direct_addresses(&self) -> Option<Vec<SocketAddr>> {
        self.network.direct_addresses().await
    }

    pub fn id(&self) -> PublicKey {
        self.network.node_id()
    }

    pub async fn import_file(&self, path: PathBuf) -> Result<()> {
        let (reply, reply_rx) = oneshot::channel();
        self.rhio_actor_tx
            .send(ToRhioActor::ImportFile { path, reply })
            .await?;
        reply_rx.await?
    }

    pub async fn ready(&mut self) -> Option<()> {
        self.ready_rx.recv().await
    }

    pub async fn shutdown(self) -> Result<()> {
        // Trigger shutdown of the main run task by activating the cancel token
        self.rhio_actor_tx.send(ToRhioActor::Shutdown).await?;
        self.network.shutdown().await?;
        self.actor_handle.await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use p2panda_net::{network::OutEvent, Config as NetworkConfig};

    use crate::config::Config;
    use crate::logging::setup_tracing;
    use crate::private_key::generate_ephemeral_private_key;

    use super::Node;

    #[tokio::test]
    async fn join_gossip_overlay() {
        setup_tracing();

        let private_key_1 = generate_ephemeral_private_key();
        let private_key_2 = generate_ephemeral_private_key();

        let config_1 = Config::default();
        let config_2 = Config::default();

        let network_config_1 = config_1.network_config;
        let network_config_2 = NetworkConfig {
            bind_port: 2023,
            ..config_2.network_config
        };

        // Spawn the first node
        let mut node_1 = Node::spawn(network_config_1, private_key_1).await.unwrap();

        // Retrieve the address of the first node
        let node_1_addr = node_1.network.endpoint().node_addr().await.unwrap();

        // Spawn the second node
        let mut node_2 = Node::spawn(network_config_2, private_key_2).await.unwrap();

        // Add the address of the first node, resulting in an automatic connection attempt under
        // the hood
        node_2.network.add_peer(node_1_addr).await.unwrap();

        let _ = node_1.ready().await;
        let _ = node_2.ready().await;

        // Subscribe to the same topic from both nodes
        let (_tx_1, mut rx_1) = node_1.network.subscribe([0; 32]).await.unwrap();
        let (_tx_2, mut rx_2) = node_2.network.subscribe([0; 32]).await.unwrap();

        // Receive the first message for both nodes
        let rx_2_msg = rx_2.recv().await.unwrap();
        let rx_1_msg = rx_1.recv().await.unwrap();

        // Ensure the gossip-overlay has been joined for the given topic
        assert_matches!(rx_1_msg, OutEvent::Ready);
        assert_matches!(rx_2_msg, OutEvent::Ready);

        node_1.shutdown().await.unwrap();
        node_2.shutdown().await.unwrap();
    }
}
