use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::Result;
use p2panda_blobs::{Blobs, MemoryStore as BlobMemoryStore};
use p2panda_core::{PrivateKey, PublicKey};
use p2panda_net::config::DEFAULT_STUN_PORT;
use p2panda_net::{LocalDiscovery, Network, NetworkBuilder};
use p2panda_store::MemoryStore as LogMemoryStore;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;
use tracing::error;

use crate::actor::{RhioActor, ToRhioActor};
use crate::config::Config;
use crate::TOPIC_ID;

pub struct Node {
    config: Config,
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

        let mut network_builder = NetworkBuilder::from_config(config.network_config.clone())
            .private_key(private_key.clone());

        match LocalDiscovery::new() {
            Ok(local) => network_builder = network_builder.discovery(local),
            Err(err) => error!("Failed to initiate local discovery: {err}"),
        }

        for relay_addr in &config.network_config.relay_addresses {
            network_builder =
                network_builder.relay(relay_addr.to_owned(), false, DEFAULT_STUN_PORT);
        }

        let (network, blobs) = Blobs::from_builder(network_builder, blob_store).await?;
        let (topic_tx, topic_rx) = network.subscribe(TOPIC_ID).await?;

        let mut rhio_actor = RhioActor::new(
            blobs.clone(),
            config.blobs_path.clone(),
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
            config,
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

    pub async fn import_file(&self, absolute_path: PathBuf) -> Result<()> {
        // We derive the relative path by stripping off the base blobs config path. This is done
        // as we want to import by absolute path, but announce relative paths (relative to the
        // configured blob directory).
        let relative_path = to_relative_path(&absolute_path, &self.config.blobs_path);
        let (reply, reply_rx) = oneshot::channel();
        self.rhio_actor_tx
            .send(ToRhioActor::ImportFile {
                absolute_path,
                relative_path,
                reply,
            })
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

fn to_relative_path(path: &PathBuf, base: &PathBuf) -> PathBuf {
    path.strip_prefix(base)
        .expect("Blob import path contains blob dir")
        .to_path_buf()
}
