use std::future::Future;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::Pin;

use anyhow::Result;
use p2panda_blobs::{Blobs, MemoryStore as BlobMemoryStore};
use p2panda_core::{PrivateKey, PublicKey, Hash};
use p2panda_net::config::Config;
use p2panda_net::{LocalDiscovery, Network, NetworkBuilder};
use p2panda_store::MemoryStore as LogMemoryStore;
use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::task::JoinHandle;
use tracing::error;

use crate::actor::{RhioActor, ToRhioActor};
use crate::messages::{Message, MessageMeta};
use crate::topic_id::TopicId;

pub struct Node<T = ()> {
    network: Network,
    rhio_actor_tx: mpsc::Sender<ToRhioActor<T>>,
    actor_handle: JoinHandle<()>,
}

impl<T> Node<T>
where
    T: Serialize + DeserializeOwned + Clone + std::fmt::Debug + Send + Sync + 'static,
{
    pub async fn spawn(config: Config, private_key: PrivateKey) -> Result<Self> {
        let (rhio_actor_tx, rhio_actor_rx) = mpsc::channel(256);

        let blob_store = BlobMemoryStore::new();
        let log_store = LogMemoryStore::default();

        let mut network_builder =
            NetworkBuilder::from_config(config.clone()).private_key(private_key.clone());

        match LocalDiscovery::new() {
            Ok(local) => network_builder = network_builder.discovery(local),
            Err(err) => error!("Failed to initiate local discovery: {err}"),
        }

        let (network, blobs) = Blobs::from_builder(network_builder, blob_store).await?;

        let mut rhio_actor =
            RhioActor::new(blobs.clone(), private_key.clone(), log_store, rhio_actor_rx);

        let actor_handle = tokio::task::spawn(async move {
            if let Err(err) = rhio_actor.run().await {
                panic!("operations actor failed: {err:?}");
            }
        });

        let node = Node {
            network,
            rhio_actor_tx,
            actor_handle,
        };

        Ok(node)
    }

    pub async fn direct_addresses(&self) -> Option<Vec<SocketAddr>> {
        self.network.direct_addresses().await
    }

    pub fn id(&self) -> PublicKey {
        self.network.node_id()
    }

    /// Import a blob from the filesystem.
    pub async fn import_blob(&self, path: PathBuf) -> Result<Hash> {
        let (reply, reply_rx) = oneshot::channel();
        self.rhio_actor_tx
            .send(ToRhioActor::ImportBlob { path, reply })
            .await?;
        reply_rx.await?
    }

    /// Export a blob to the filesystem.
    pub async fn export_blob(&self, hash: Hash, path: PathBuf) -> Result<()> {
        let (reply, reply_rx) = oneshot::channel();
        self.rhio_actor_tx
            .send(ToRhioActor::ExportBlob { hash, path, reply })
            .await?;
        reply_rx.await?
    }

    /// Download a blob from the network.
    pub async fn download_blob(&self, hash: Hash) -> Result<()> {
        let (reply, reply_rx) = oneshot::channel();
        self.rhio_actor_tx
            .send(ToRhioActor::DownloadBlob { hash, reply })
            .await?;
        reply_rx.await?
    }

    /// Subscribe to a gossip topic.
    ///
    /// Returns a sender, receiver and future which resolves once the gossip overlay is ready.
    pub async fn subscribe(
        &self,
        topic: TopicId,
    ) -> Result<(
        TopicSender<T>,
        broadcast::Receiver<(Message<T>, MessageMeta)>,
        Pin<Box<dyn Future<Output = ()> + Send>>,
    )> {
        let (topic_tx, topic_rx) = self.network.subscribe(topic.into()).await?;
        let (reply, reply_rx) = oneshot::channel();
        self.rhio_actor_tx
            .send(ToRhioActor::Subscribe {
                topic,
                topic_tx,
                topic_rx,
                reply,
            })
            .await?;
        let result = reply_rx.await?;
        let tx = TopicSender::new(topic, self.rhio_actor_tx.clone());
        result.map(|(rx, ready)| (tx, rx, ready))
    }

    pub async fn shutdown(self) -> Result<()> {
        // Trigger shutdown of the main run task by activating the cancel token
        self.rhio_actor_tx.send(ToRhioActor::Shutdown).await?;
        self.network.shutdown().await?;
        self.actor_handle.await?;
        Ok(())
    }
}

pub struct TopicSender<T> {
    topic_id: TopicId,
    tx: mpsc::Sender<ToRhioActor<T>>,
    _phantom: PhantomData<T>,
}

impl<T> TopicSender<T>
where
    T: Send + Sync + 'static,
{
    pub fn new(topic_id: TopicId, tx: mpsc::Sender<ToRhioActor<T>>) -> TopicSender<T> {
        TopicSender {
            topic_id,
            tx,
            _phantom: PhantomData::<T>,
        }
    }

    pub async fn send(&self, message: Message<T>) -> Result<MessageMeta> {
        let (reply, reply_rx) = oneshot::channel();
        self.tx
            .send(ToRhioActor::PublishEvent {
                topic: self.topic_id,
                message,
                reply,
            })
            .await?;
        reply_rx.await?
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use p2panda_net::{network::OutEvent, Config};

    use crate::logging::setup_tracing;
    use crate::private_key::generate_ephemeral_private_key;

    use super::Node;

    #[tokio::test]
    async fn join_gossip_overlay() {
        setup_tracing();

        let private_key_1 = generate_ephemeral_private_key();
        let private_key_2 = generate_ephemeral_private_key();

        let network_config_1 = Config::default();
        let network_config_2 = Config {
            bind_port: 2023,
            ..Default::default()
        };

        // Spawn the first node
        let node_1: Node<()> = Node::spawn(network_config_1, private_key_1).await.unwrap();

        // Retrieve the address of the first node
        let node_1_addr = node_1.network.endpoint().node_addr().await.unwrap();

        // Spawn the second node
        let node_2: Node<()> = Node::spawn(network_config_2, private_key_2).await.unwrap();

        // Retrieve the address of the second node
        let node_2_addr = node_2.network.endpoint().node_addr().await.unwrap();

        // Add the address of the first node, resulting in an automatic connection attempt under
        // the hood
        //
        // Note that a connection attempt will still occur without this explicit call to
        // `add_peer()` if the nodes are on the same local network and therefore discoverable via
        // mDNS
        node_2.network.add_peer(node_1_addr.clone()).await.unwrap();

        // Subscribe to the same topic from both nodes
        let (_tx_1, mut rx_1) = node_1.network.subscribe([0; 32]).await.unwrap();
        let (_tx_2, mut rx_2) = node_2.network.subscribe([0; 32]).await.unwrap();

        tokio::time::sleep(Duration::from_secs(2)).await;

        // Receive the first message for both nodes
        let rx_2_msg = rx_2.recv().await.unwrap();
        let rx_1_msg = rx_1.recv().await.unwrap();

        // Ensure the gossip-overlay has been joined for the given topic
        assert!(matches!(rx_1_msg, OutEvent::Ready));
        assert!(matches!(rx_2_msg, OutEvent::Ready));

        // Return a list of peers known to the first node
        let known_peers_1 = node_1.network.known_peers().await.unwrap();

        // Ensure the first node knows the direct addresses of the second node.
        // This information was not manually provided and has thus been learned via gossip.
        assert_eq!(
            known_peers_1[0].info.direct_addresses,
            node_2_addr.info.direct_addresses
        );

        node_1.shutdown().await.unwrap();
        node_2.shutdown().await.unwrap();
    }
}
