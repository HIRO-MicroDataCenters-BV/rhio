use std::path::PathBuf;

use anyhow::Result;
use futures_util::Stream;
use iroh_net::endpoint::DirectAddr;
use iroh_net::NodeId;
use p2panda_blobs::{Blobs, DownloadBlobEvent, ImportBlobEvent, MemoryStore};
use p2panda_core::{Body, Hash, Header, PrivateKey};
use p2panda_net::config::Config;
use p2panda_net::network::{InEvent, OutEvent};
use p2panda_net::{LocalDiscovery, Network, NetworkBuilder, TopicId};
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast::Receiver;
use tokio::sync::mpsc::Sender;

use crate::extensions::Extensions;
use crate::TOPIC_ID;

#[derive(Serialize, Deserialize)]
pub struct Message {
    pub header: Header<Extensions>,
    pub text: String,
}

pub struct Node {
    #[allow(dead_code)]
    config: Config,
    network: Network,
    #[allow(dead_code)]
    blobs: Blobs<MemoryStore>,
    gossip_tx: Sender<InEvent>,
}

impl Node {
    pub async fn spawn(
        config: Config,
        private_key: PrivateKey,
    ) -> Result<(Self, Receiver<OutEvent>)> {
        let blob_store = MemoryStore::new();

        let network_builder = NetworkBuilder::from_config(config.clone())
            .private_key(private_key)
            .discovery(LocalDiscovery::new()?);

        let (network, blobs) = Blobs::from_builder(network_builder, blob_store).await?;

        let (tx, rx) = network.subscribe(TOPIC_ID).await?;

        let node = Node {
            config,
            network,
            blobs,
            gossip_tx: tx,
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

    pub async fn send_message(&self, private_key: &PrivateKey, text: &str) -> Result<()> {
        let mut body_bytes: Vec<u8> = Vec::new();
        ciborium::ser::into_writer(text, &mut body_bytes)?;

        let body = Body::new(&body_bytes);
        let mut header = Header {
            version: 1,
            public_key: private_key.public_key(),
            signature: None,
            payload_size: body.size(),
            payload_hash: Some(body.hash()),
            timestamp: 0,
            seq_num: 0,
            backlink: None,
            previous: vec![],
            extensions: Some(Extensions::default()),
        };
        header.sign(&private_key);

        let message = Message {
            header,
            text: text.to_string()
        };

        let mut bytes = Vec::new();
        ciborium::ser::into_writer(&message, &mut bytes)?;

        self.gossip_tx.send(InEvent::Message { bytes }).await?;

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
