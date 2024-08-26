use std::future::Future;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::Pin;

use anyhow::{anyhow, Context, Result};
use async_nats::jetstream::Context as JetstreamContext;
use async_nats::{Client as NatsClient, ConnectOptions};
use p2panda_blobs::{Blobs, FilesystemStore, MemoryStore as BlobsMemoryStore};
use p2panda_core::{Hash, PrivateKey, PublicKey};
use p2panda_net::{
    Config as NetworkConfig, LocalDiscovery, Network, NetworkBuilder, SharedAbortingJoinHandle,
};
use p2panda_store::MemoryStore as LogMemoryStore;
use s3::Region;
use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio_util::task::LocalPoolHandle;
use tracing::error;

use crate::actor::{RhioActor, ToRhioActor};
use crate::config::{Config, ImportPath};
use crate::extensions::{LogId, RhioExtensions};
use crate::messages::{Message, MessageMeta};
use crate::topic_id::TopicId;

// @TODO: Give rhio a cool network id
const RHIO_NETWORK_ID: [u8; 32] = [0; 32];

/// p2panda network node which handles connecting to other peers, syncing operations over topics
/// and blobs using the bao protocol.
#[derive(Clone)]
pub struct Node<T = Vec<u8>> {
    pub config: Config,
    network: Network,
    actor_tx: mpsc::Sender<ToRhioActor<T>>,
    actor_handle: SharedAbortingJoinHandle<()>,
    nats_client: NatsClient,
    nats_jetstream: JetstreamContext,
}

impl<T> Node<T>
where
    T: Serialize + DeserializeOwned + Clone + std::fmt::Debug + Send + Sync + 'static,
{
    /// Configure and spawn a node.
    pub async fn spawn(config: Config, private_key: PrivateKey) -> Result<Self> {
        let pool = LocalPoolHandle::new(1);
        let (actor_tx, rhio_actor_rx) = mpsc::channel(256);

        let log_store: LogMemoryStore<LogId, RhioExtensions> = LogMemoryStore::new();

        let mut network_config = NetworkConfig {
            bind_port: config.node.bind_port,
            network_id: RHIO_NETWORK_ID,
            ..Default::default()
        };

        for node in &config.node.known_nodes {
            network_config.direct_node_addresses.push((
                node.public_key,
                node.direct_addresses.clone(),
                None,
            ));
        }

        let mut network_builder =
            NetworkBuilder::from_config(network_config).private_key(private_key.clone());

        // @TODO: Remove mDNS, it's not needed in rhio
        match LocalDiscovery::new() {
            Ok(local) => network_builder = network_builder.discovery(local),
            Err(err) => error!("Failed to initiate local discovery: {err}"),
        }

        let (network, actor_handle) = if let Some(blobs_dir) = &config.blobs_dir {
            // Spawn a rhio actor backed by a filesystem blob store
            let blob_store = FilesystemStore::load(blobs_dir).await?;
            let (network, blobs) = Blobs::from_builder(network_builder, blob_store).await?;
            let actor_handle = RhioActor::spawn(
                private_key.clone(),
                blobs,
                log_store,
                rhio_actor_rx,
                pool.clone(),
            );
            (network, actor_handle)
        } else {
            // Spawn a rhio actor backed by an in memory blob store
            let blob_store = BlobsMemoryStore::new();
            let (network, blobs) = Blobs::from_builder(network_builder, blob_store).await?;
            let actor_handle = RhioActor::spawn(
                private_key.clone(),
                blobs,
                log_store,
                rhio_actor_rx,
                pool.clone(),
            );
            (network, actor_handle)
        };

        // @TODO: Add auth options to NATS server config
        let nats_client =
            async_nats::connect_with_options(config.nats.endpoint.clone(), ConnectOptions::new())
                .await
                .context("connecting to NATS server")?;
        let nats_jetstream = async_nats::jetstream::new(nats_client.clone());

        let node = Node {
            config,
            network,
            actor_tx,
            actor_handle: actor_handle.into(),
            nats_client,
            nats_jetstream,
        };

        Ok(node)
    }

    /// Returns the PublicKey of this node which is used as it's ID.
    ///
    /// This ID is the unique addressing information of this node and other peers must know it to
    /// be able to connect to this node.
    pub fn id(&self) -> PublicKey {
        self.network.node_id()
    }

    /// Returns the direct addresses of this Node.
    ///
    /// The direct addresses of the Node are those that could be used by other nodes to establish
    /// direct connectivity, depending on the network situation. The yielded lists of direct
    /// addresses contain both the locally-bound addresses and the Node's publicly reachable
    /// addresses discovered through mechanisms such as STUN and port mapping. Hence usually only a
    /// subset of these will be applicable to a certain remote node.
    pub async fn direct_addresses(&self) -> Option<Vec<SocketAddr>> {
        self.network.direct_addresses().await
    }

    /// Import a blob into the node's blob store and sync it with the MinIO database.
    pub async fn import_blob(&self, import_path: ImportPath) -> Result<Hash> {
        let hash = match import_path {
            // Import a file from the local filesystem
            ImportPath::File(path) => self.import_blob_filesystem(path.clone()).await?,
            // Import a file from the given url
            ImportPath::Url(url) => self.import_blob_url(url.clone()).await?,
        };

        // Export the blob data from the blob store to a minio bucket
        if self.config.minio.credentials.is_some() {
            self.export_blob_minio(
                hash,
                self.config.minio.region.clone(),
                self.config.minio.endpoint.clone(),
                self.config.minio.bucket_name.clone(),
            )
            .await?;
        }

        Ok(hash)
    }

    /// Import a blob from the filesystem.
    ///
    /// Add a blob from a path on the local filesystem to the dedicated blob store and make it
    /// available on the network identified by it's BLAKE3 hash.
    async fn import_blob_filesystem(&self, path: PathBuf) -> Result<Hash> {
        let (reply, reply_rx) = oneshot::channel();
        self.actor_tx
            .send(ToRhioActor::ImportFile {
                path: path.clone(),
                reply,
            })
            .await?;
        reply_rx.await?
    }

    /// Import a blob from a url.
    ///
    /// Download a blob from a url, move it into the dedicated blob store and make it available on
    /// the network identified by it's BLAKE3 hash.
    async fn import_blob_url(&self, url: String) -> Result<Hash> {
        let (reply, reply_rx) = oneshot::channel();
        self.actor_tx
            .send(ToRhioActor::ImportUrl { url, reply })
            .await?;
        reply_rx.await?
    }

    /// Export a blob to the filesystem.
    ///
    /// Copies an existing blob from the blob store to a location on the filesystem.
    pub async fn export_blob_filesystem(&self, hash: Hash, path: PathBuf) -> Result<()> {
        let (reply, reply_rx) = oneshot::channel();
        self.actor_tx
            .send(ToRhioActor::ExportBlobFilesystem { hash, path, reply })
            .await?;
        reply_rx.await?
    }

    /// Export a blob to a minio bucket.
    ///
    /// Copies an existing blob from the blob store to the provided minio bucket.
    pub async fn export_blob_minio(
        &self,
        hash: Hash,
        region: String,
        endpoint: String,
        bucket_name: String,
    ) -> Result<()> {
        let Some(credentials) = &self.config.minio.credentials else {
            return Err(anyhow!("No minio credentials provided"));
        };

        // Get the blobs entry from the blob store
        let (reply, reply_rx) = oneshot::channel();
        self.actor_tx
            .send(ToRhioActor::ExportBlobMinio {
                hash,
                bucket_name,
                region: Region::Custom { region, endpoint },
                credentials: credentials.clone(),
                reply,
            })
            .await?;
        reply_rx.await?
    }

    /// Download a blob from the network.
    ///
    /// Attempt to download a blob from peers on the network and place it into the nodes MinIO
    /// bucket.
    pub async fn download_blob(&self, hash: Hash) -> Result<()> {
        let (reply, reply_rx) = oneshot::channel();
        self.actor_tx
            .send(ToRhioActor::DownloadBlob { hash, reply })
            .await?;
        let result = reply_rx.await?;
        result?;

        if self.config.minio.credentials.is_some() {
            self.export_blob_minio(
                hash,
                self.config.minio.region.clone(),
                self.config.minio.endpoint.clone(),
                self.config.minio.bucket_name.clone(),
            )
            .await?;
        }

        Ok(())
    }

    /// Subscribe to a gossip topic.
    ///
    /// Returns a sender for broadcasting messages to all peers subscribed to this topic, a
    /// receiver where messages can be awaited, and future which resolves once the gossip overlay
    /// is ready.
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
        self.actor_tx
            .send(ToRhioActor::Subscribe {
                topic,
                topic_tx,
                topic_rx,
                reply,
            })
            .await?;
        let result = reply_rx.await?;
        let tx = TopicSender::new(topic, self.actor_tx.clone());
        result.map(|(rx, ready)| (tx, rx, ready))
    }

    /// Shutdown the node.
    pub async fn shutdown(self) -> Result<()> {
        // Trigger shutdown of the main run task by activating the cancel token
        self.actor_tx.send(ToRhioActor::Shutdown).await?;
        self.network.shutdown().await?;
        self.actor_handle
            .await
            .map_err(|err| anyhow::anyhow!("{err}"))?;
        Ok(())
    }
}

pub struct TopicSender<T = Vec<u8>> {
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

    /// Broadcast a message to all peers subscribing to the same topic.
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

    /// Broadcast a blob announcement message to all peers subscribing to the same topic.
    pub async fn announce_blob(&self, hash: Hash) -> Result<MessageMeta> {
        let (reply, reply_rx) = oneshot::channel();
        self.tx
            .send(ToRhioActor::PublishEvent {
                topic: self.topic_id,
                message: Message::BlobAnnouncement(hash),
                reply,
            })
            .await?;
        reply_rx.await?
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use p2panda_net::network::OutEvent;

    use crate::config::Config;
    use crate::private_key::generate_ephemeral_private_key;

    use super::Node;

    #[tokio::test]
    async fn join_gossip_overlay() {
        let private_key_1 = generate_ephemeral_private_key();
        let private_key_2 = generate_ephemeral_private_key();

        let config_1 = Config::default();
        let mut config_2 = Config::default();
        config_2.node.bind_port = 2023;

        // Spawn the first node
        let node_1: Node<()> = Node::spawn(config_1, private_key_1).await.unwrap();

        // Retrieve the address of the first node
        let node_1_addr = node_1.network.endpoint().node_addr().await.unwrap();

        // Spawn the second node
        let node_2: Node<()> = Node::spawn(config_2, private_key_2).await.unwrap();

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
