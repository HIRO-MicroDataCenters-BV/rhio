use std::path::PathBuf;

use anyhow::Result;
use iroh_net::endpoint::DirectAddr;
use iroh_net::NodeId;
use p2panda_blobs::{Blobs, MemoryStore as BlobMemoryStore};
use p2panda_core::PrivateKey;
use p2panda_net::config::Config;
use p2panda_net::{LocalDiscovery, Network, NetworkBuilder};
use p2panda_store::MemoryStore as LogMemoryStore;
use tokio::sync::mpsc;
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

    #[allow(dead_code)]
    pub async fn direct_addresses(&self) -> Option<Vec<DirectAddr>> {
        self.network.direct_addresses().await
    }

    pub fn node_id(&self) -> NodeId {
        self.network.node_id()
    }

    pub async fn announce_new_file(&self, path: PathBuf) -> Result<()> {
        self.rhio_actor_tx
            .send(ToRhioActor::NewFile { path })
            .await?;
        Ok(())
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
