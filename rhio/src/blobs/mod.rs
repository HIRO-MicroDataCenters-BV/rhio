mod actor;

use std::path::PathBuf;

use anyhow::{anyhow, Result};
use iroh_blobs::store::Store;
use p2panda_blobs::Blobs as BlobsHandler;
use p2panda_core::Hash;
use p2panda_net::SharedAbortingJoinHandle;
use s3::Region;
use tokio::sync::{mpsc, oneshot};
use tokio_util::task::LocalPoolHandle;
use tracing::error;

use crate::blobs::actor::{BlobsActor, ToBlobsActor};
use crate::config::{ImportPath, MinioConfig};

#[derive(Debug)]
pub struct Blobs {
    config: MinioConfig,
    blobs_actor_tx: mpsc::Sender<ToBlobsActor>,
    #[allow(dead_code)]
    actor_handle: SharedAbortingJoinHandle<()>,
}

impl Blobs {
    pub fn new<S: Store>(config: MinioConfig, blobs_handler: BlobsHandler<S>) -> Self {
        let (blobs_actor_tx, blobs_actor_rx) = mpsc::channel(256);
        let blobs_actor = BlobsActor::new(blobs_handler, blobs_actor_rx);
        let pool = LocalPoolHandle::new(1);

        let actor_handle = pool.spawn_pinned(|| async move {
            if let Err(err) = blobs_actor.run().await {
                error!("blobs actor failed: {err:?}");
            }
        });

        Self {
            config,
            blobs_actor_tx,
            actor_handle: actor_handle.into(),
        }
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
        if self.config.credentials.is_some() {
            self.export_blob_minio(
                hash,
                self.config.region.clone(),
                self.config.endpoint.clone(),
                self.config.bucket_name.clone(),
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
        self.blobs_actor_tx
            .send(ToBlobsActor::ImportFile {
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
        self.blobs_actor_tx
            .send(ToBlobsActor::ImportUrl { url, reply })
            .await?;
        reply_rx.await?
    }

    /// Export a blob to the filesystem.
    ///
    /// Copies an existing blob from the blob store to a location on the filesystem.
    pub async fn export_blob_filesystem(&self, hash: Hash, path: PathBuf) -> Result<()> {
        let (reply, reply_rx) = oneshot::channel();
        self.blobs_actor_tx
            .send(ToBlobsActor::ExportBlobFilesystem { hash, path, reply })
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
        let Some(credentials) = &self.config.credentials else {
            return Err(anyhow!("No minio credentials provided"));
        };

        // Get the blobs entry from the blob store
        let (reply, reply_rx) = oneshot::channel();
        self.blobs_actor_tx
            .send(ToBlobsActor::ExportBlobMinio {
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
        self.blobs_actor_tx
            .send(ToBlobsActor::DownloadBlob { hash, reply })
            .await?;
        let result = reply_rx.await?;
        result?;

        if self.config.credentials.is_some() {
            self.export_blob_minio(
                hash,
                self.config.region.clone(),
                self.config.endpoint.clone(),
                self.config.bucket_name.clone(),
            )
            .await?;
        }

        Ok(())
    }

    pub async fn shutdown(&self) -> Result<()> {
        let (reply, reply_rx) = oneshot::channel();
        self.blobs_actor_tx
            .send(ToBlobsActor::Shutdown { reply })
            .await?;
        reply_rx.await?;
        Ok(())
    }
}
