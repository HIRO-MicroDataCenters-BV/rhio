use std::path::PathBuf;
use std::time::SystemTime;

use anyhow::Result;
use futures_util::Stream;
use iroh_net::endpoint::DirectAddr;
use iroh_net::NodeId;
use p2panda_blobs::{Blobs, DownloadBlobEvent, ImportBlobEvent, MemoryStore as BlobMemoryStore};
use p2panda_core::{Body, Hash, Header, Operation, PrivateKey};
use p2panda_net::config::Config;
use p2panda_net::network::{InEvent, OutEvent};
use p2panda_net::{LocalDiscovery, Network, NetworkBuilder, TopicId};
use p2panda_store::{LogId, LogStore, MemoryStore as LogMemoryStore, OperationStore};
use serde::{Deserialize, Serialize};
use tokio::sync::broadcast::Receiver;
use tokio::sync::mpsc::Sender;

use crate::extensions::Extensions;
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
    logs: LogMemoryStore<Extensions>,
    gossip_tx: Sender<InEvent>,
}

impl Node {
    pub async fn spawn(
        config: Config,
        private_key: PrivateKey,
    ) -> Result<(Self, Receiver<OutEvent>)> {
        let blob_store = BlobMemoryStore::new();
        let log_store = LogMemoryStore::default();

        let network_builder = NetworkBuilder::from_config(config.clone())
            .private_key(private_key)
            .discovery(LocalDiscovery::new()?);

        let (network, blobs) = Blobs::from_builder(network_builder, blob_store).await?;

        let (tx, rx) = network.subscribe(TOPIC_ID).await?;

        let node = Node {
            config,
            network,
            blobs,
            logs: log_store,
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

    pub async fn send_message(&mut self, private_key: &PrivateKey, text: &str) -> Result<()> {
        let mut body_bytes: Vec<u8> = Vec::new();
        ciborium::ser::into_writer(text, &mut body_bytes)?;

        let public_key = private_key.public_key();
        let latest_operation = self
            .logs
            .latest_operation(private_key.public_key(), public_key.to_string().into())?;

        let (seq_num, backlink) = match latest_operation {
            Some(operation) => (operation.header.seq_num + 1, Some(operation.hash)),
            None => (0, None),
        };

        let timestamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)?
            .as_secs();

        let body = Body::new(&body_bytes);
        let mut header = Header {
            version: 1,
            public_key: private_key.public_key(),
            signature: None,
            payload_size: body.size(),
            payload_hash: Some(body.hash()),
            timestamp,
            seq_num,
            backlink,
            previous: vec![],
            extensions: Some(Extensions::default()),
        };
        header.sign(&private_key);

        let operation = Operation {
            hash: header.hash(),
            header: header.clone(),
            body: Some(body),
        };

        self.logs.insert_operation(operation);

        let message = Message {
            header,
            text: text.to_string(),
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
