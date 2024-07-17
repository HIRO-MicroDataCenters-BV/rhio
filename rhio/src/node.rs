use std::path::PathBuf;

use anyhow::Result;
use futures_util::Stream;
use iroh_net::endpoint::DirectAddr;
use iroh_net::NodeId;
use p2panda_blobs::{Blobs, DownloadBlobEvent, ImportBlobEvent, MemoryStore as BlobMemoryStore};
use p2panda_core::{Hash, Header, PrivateKey};
use p2panda_net::config::Config;
use p2panda_net::network::{InEvent, OutEvent};
use p2panda_net::{LocalDiscovery, Network, NetworkBuilder, TopicId};
use p2panda_store::MemoryStore as LogMemoryStore;
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast::Receiver;
use tokio::sync::mpsc::{self, Sender};
use tokio::task::JoinHandle;

use crate::extensions::Extensions;
use crate::operations::{OperationsActor, ToOperationActor};
use crate::TOPIC_ID;

#[derive(Serialize, Deserialize)]
pub struct Message {
    pub text: String,
    pub header: Header<Extensions>,
}

pub struct Node {
    #[allow(dead_code)]
    config: Config,
    network: Network,
    #[allow(dead_code)]
    blobs: Blobs<BlobMemoryStore>,
    operations_actor_tx: mpsc::Sender<ToOperationActor>,
    actor_handle: JoinHandle<()>,
}

impl Node {
    pub async fn spawn(
        config: Config,
        private_key: PrivateKey,
    ) -> Result<(Self, Receiver<OutEvent>)> {
        let (operations_actor_tx, operations_actor_rx) = mpsc::channel(256);

        let blob_store = BlobMemoryStore::new();
        let log_store = LogMemoryStore::default();

        let network_builder = NetworkBuilder::from_config(config.clone())
            .private_key(private_key.clone())
            .discovery(LocalDiscovery::new()?);

        let (network, blobs) = Blobs::from_builder(network_builder, blob_store).await?;

        let (tx, rx) = network.subscribe(TOPIC_ID).await?;

        let mut operations_actor =
            OperationsActor::new(private_key.clone(), log_store, tx, operations_actor_rx);

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
        };

        Ok((node, rx))
    }

    #[allow(dead_code)]
    pub async fn direct_addresses(&self) -> Option<Vec<DirectAddr>> {
        self.network.direct_addresses().await
    }

    #[allow(dead_code)]
    pub fn node_id(&self) -> NodeId {
        self.network.node_id()
    }

    pub async fn subscribe(
        &mut self,
        topic: TopicId,
    ) -> Result<(Sender<InEvent>, Receiver<OutEvent>)> {
        self.network.subscribe(topic).await
    }

    pub async fn send_message(&self, text: &str) -> Result<()> {
        self.operations_actor_tx
            .send(ToOperationActor::Send {
                text: text.to_string(),
            })
            .await?;
        Ok(())
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
