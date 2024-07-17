use std::path::PathBuf;

use anyhow::Result;
use futures_util::Stream;
use iroh_net::endpoint::DirectAddr;
use iroh_net::NodeId;
use p2panda_blobs::{Blobs, DownloadBlobEvent, ImportBlobEvent, MemoryStore as BlobMemoryStore};
use p2panda_core::{Hash, PrivateKey};
use p2panda_net::config::Config;
use p2panda_net::{LocalDiscovery, Network, NetworkBuilder};
use p2panda_store::MemoryStore as LogMemoryStore;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::message::Message;
use crate::operations::{OperationsActor, ToOperationActor};
use crate::TOPIC_ID;

pub struct Node {
    config: Config,
    network: Network,
    blobs: Blobs<BlobMemoryStore>,
    operations_actor_tx: mpsc::Sender<ToOperationActor>,
    actor_handle: JoinHandle<()>,
    ready_rx: mpsc::Receiver<()>,
}

impl Node {
    pub async fn spawn(config: Config, private_key: PrivateKey) -> Result<Self> {
        let (operations_actor_tx, operations_actor_rx) = mpsc::channel(256);
        let (ready_tx, ready_rx) = mpsc::channel::<()>(1);

        let blob_store = BlobMemoryStore::new();
        let log_store = LogMemoryStore::default();

        let network_builder = NetworkBuilder::from_config(config.clone())
            .private_key(private_key.clone())
            .discovery(LocalDiscovery::new()?);

        let (network, blobs) = Blobs::from_builder(network_builder, blob_store).await?;
        let (topic_tx, topic_rx) = network.subscribe(TOPIC_ID).await?;

        let mut operations_actor = OperationsActor::new(
            blobs.clone(),
            private_key.clone(),
            log_store,
            topic_tx,
            topic_rx,
            operations_actor_rx,
            ready_tx,
        );

        let actor_handle = tokio::task::spawn(async move {
            if let Err(err) = operations_actor.run().await {
                panic!("engine actor failed: {err:?}");
            }
        });

        let node = Node {
            config,
            network,
            blobs,
            operations_actor_tx,
            actor_handle,
            ready_rx,
        };

        Ok(node)
    }

    pub async fn direct_addresses(&self) -> Option<Vec<DirectAddr>> {
        self.network.direct_addresses().await
    }

    pub fn node_id(&self) -> NodeId {
        self.network.node_id()
    }

    pub async fn send_message(&self, message: Message) -> Result<()> {
        let (reply, reply_rx) = oneshot::channel();
        self.operations_actor_tx
            .send(ToOperationActor::SendMessage { message, reply })
            .await?;
        reply_rx.await?
    }

    pub async fn import_blob(&self, path: PathBuf) -> impl Stream<Item = ImportBlobEvent> {
        self.blobs.import_blob(path).await
    }

    pub async fn download_blob(&self, hash: Hash) -> impl Stream<Item = DownloadBlobEvent> {
        self.blobs.download_blob(hash).await
    }

    pub async fn ready(&mut self) -> Option<()> {
        self.ready_rx.recv().await
    }

    pub async fn shutdown(self) -> Result<()> {
        // Trigger shutdown of the main run task by activating the cancel token
        self.operations_actor_tx
            .send(ToOperationActor::Shutdown)
            .await?;
        self.network.shutdown().await?;
        self.actor_handle.await?;
        Ok(())
    }
}
