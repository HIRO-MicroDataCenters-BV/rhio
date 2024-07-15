use std::net::{self, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use futures_lite::{Stream, StreamExt};
use iroh_blobs::downloader::Downloader;
use iroh_blobs::get::db::DownloadProgress;
use iroh_blobs::protocol::{Closed, ALPN as BLOBS_ALPN};
use iroh_blobs::provider::AddProgress;
use iroh_blobs::store::mem::Store as MemoryStore;
use iroh_blobs::store::Store;
use iroh_blobs::util::progress::{FlumeProgressSender, ProgressSender};
use iroh_blobs::{BlobFormat, Hash, HashAndFormat};
use iroh_gossip::net::{Gossip, GOSSIP_ALPN};
use iroh_net::endpoint::{Connection, DirectAddr, TransportConfig};
use iroh_net::key::SecretKey;
use iroh_net::relay::RelayMode;
use iroh_net::util::SharedAbortingJoinHandle;
use iroh_net::{Endpoint, NodeAddr, NodeId};
use p2panda_core::identity::PublicKey;
use p2panda_core::PrivateKey;
use p2panda_net::{network, LocalDiscovery, Network, NetworkBuilder};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tokio_util::task::LocalPoolHandle;
use tracing::{debug, error, error_span, warn, Instrument};

use crate::blobs::{add_from_path, download_queued, BlobAddPathResponse, BlobDownloadResponse};
use crate::config::Config;
use crate::protocol::{BlobsProtocol, BubuProtocol, BUBU_ALPN};

const MAX_RPC_STREAMS: u32 = 1024;
const MAX_CONNECTIONS: u32 = 1024;
const NETWORK_ID: [u8; 32] = [0; 32];

/// How long we wait at most for some endpoints to be discovered.
const ENDPOINT_WAIT: Duration = Duration::from_secs(5);

pub struct Node<D> {
    config: Config,
    db: D,
    downloader: Downloader,
    network: Network,
    pool_handle: LocalPoolHandle,
}

impl Node<MemoryStore> {
    pub async fn spawn(config: Config) -> Result<Self> {
        let private_key = PrivateKey::new();

        let mut network_builder = NetworkBuilder::new(NETWORK_ID)
            .private_key(private_key.clone())
            .bind_port(config.bind_port)
            .discovery(LocalDiscovery::new()?)
            .gossip(Default::default());
        // @TODO: configure relay node(s).

        for direct_node in &config.direct_node_addresses {
            // @TODO: update config to directly parse to p2panda types.
            let public_key =
                PublicKey::from_bytes(direct_node.node_id.as_bytes()).expect("invalid public key");
            network_builder = network_builder.direct_address(
                public_key,
                direct_node
                    .direct_addresses()
                    .cloned()
                    .collect::<Vec<SocketAddr>>(),
            )
        }

        let pool_handle = LocalPoolHandle::new(num_cpus::get());
        let db = MemoryStore::new();

        network_builder = network_builder.protocol(
            BLOBS_ALPN,
            BlobsProtocol::new(db.clone(), pool_handle.clone()),
        );

        let network = network_builder.build().await?;

        let downloader =
            Downloader::new(db.clone(), network.endpoint().clone(), pool_handle.clone());

        let node = Node {
            config,
            network,
            db,
            downloader,
            pool_handle,
        };

        Ok(node)
    }

    pub async fn direct_addresses(&self) -> Option<Vec<DirectAddr>> {
        self.network.endpoint().direct_addresses().next().await
    }

    /// Returns the public key of the node.
    pub fn node_id(&self) -> NodeId {
        self.network.endpoint().node_id()
    }

    pub async fn connect(&self, node_addr: NodeAddr) -> Result<Connection> {
        self.network.endpoint().connect(node_addr, BUBU_ALPN).await
    }

    pub async fn add_blob(&self, path: PathBuf) -> impl Stream<Item = BlobAddPathResponse> {
        let (sender, receiver) = flume::bounded(32);
        let db = self.db.clone();

        {
            let sender = sender.clone();
            self.pool_handle.spawn_pinned(|| async move {
                if let Err(e) = add_from_path(db, path, sender.clone()).await {
                    sender.send_async(AddProgress::Abort(e.into())).await.ok();
                }
            });
        }

        receiver.into_stream().map(BlobAddPathResponse)
    }

    pub async fn blob_download(&self, hash: Hash) -> impl Stream<Item = BlobDownloadResponse> {
        let (sender, receiver) = flume::bounded(1024);
        let progress = FlumeProgressSender::new(sender);
        let downloader = self.downloader.clone();
        let endpoint = self.network.endpoint().clone();
        let nodes = self.config.direct_node_addresses.clone();
        let format = BlobFormat::Raw;
        let hash_and_format = HashAndFormat { hash, format };

        self.pool_handle.spawn_pinned(move || async move {
            match download_queued(
                endpoint,
                &downloader,
                hash_and_format,
                nodes,
                progress.clone(),
            )
            .await
            {
                Ok(stats) => {
                    progress.send(DownloadProgress::AllDone(stats)).await.ok();
                }
                Err(err) => {
                    progress
                        .send(DownloadProgress::Abort(err.into()))
                        .await
                        .ok();
                }
            }
        });

        receiver.into_stream().map(BlobDownloadResponse)
    }

    pub async fn shutdown(self) -> Result<()> {
        // Trigger shutdown of the main run task by activating the cancel token.
        self.network.shutdown().await;

        Ok(())
    }
}
