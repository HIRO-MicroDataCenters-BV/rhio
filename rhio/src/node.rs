use std::path::PathBuf;

use anyhow::Result;
use futures_lite::{Stream, StreamExt};
use iroh_blobs::downloader::Downloader;
use iroh_blobs::get::db::DownloadProgress;
use iroh_blobs::protocol::ALPN as BLOBS_ALPN;
use iroh_blobs::provider::AddProgress;
use iroh_blobs::store::mem::Store as MemoryStore;
use iroh_blobs::util::progress::{FlumeProgressSender, ProgressSender};
use iroh_blobs::{BlobFormat, Hash, HashAndFormat};
use iroh_net::endpoint::DirectAddr;
use iroh_net::NodeId;
use p2panda_core::PrivateKey;
use p2panda_net::config::{to_node_addr, Config};
use p2panda_net::{LocalDiscovery, Network, NetworkBuilder};
use tokio_util::task::LocalPoolHandle;

use crate::blobs::{add_from_path, download_queued, BlobAddPathResponse, BlobDownloadResponse};
use crate::protocol::BlobsProtocol;

#[allow(dead_code)]
pub struct Node<D> {
    config: Config,
    db: D,
    downloader: Downloader,
    network: Network,
    pool_handle: LocalPoolHandle,
}

impl Node<MemoryStore> {
    pub async fn spawn(config: Config, private_key: PrivateKey) -> Result<Self> {
        let pool_handle = LocalPoolHandle::new(num_cpus::get());
        let db = MemoryStore::new();

        let network_builder = NetworkBuilder::from_config(config.clone())
            .private_key(private_key)
            .discovery(LocalDiscovery::new()?)
            .protocol(
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
        self.network.direct_addresses().await
    }

    /// Returns the public key of the node.
    pub fn node_id(&self) -> NodeId {
        self.network.node_id()
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
        let nodes = self
            .config
            .direct_node_addresses
            .iter()
            .map(|(public_key, addresses)| to_node_addr(public_key, addresses))
            .collect();
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
        // Trigger shutdown of the main run task by activating the cancel token
        self.network.shutdown().await?;
        Ok(())
    }
}
