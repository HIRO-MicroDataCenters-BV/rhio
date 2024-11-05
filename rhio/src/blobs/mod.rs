mod actor;

use std::path::PathBuf;

use anyhow::{anyhow, Result};
use futures_util::future::{MapErr, Shared};
use futures_util::{FutureExt, TryFutureExt};
use iroh_blobs::store::Store;
use p2panda_blobs::Blobs as BlobsHandler;
use p2panda_core::Hash;
use s3::Region;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinError;
use tokio_util::task::{AbortOnDropHandle, LocalPoolHandle};
use tracing::error;

use crate::blobs::actor::{BlobsActor, ToBlobsActor};
use crate::config::S3Config;
use crate::topic::Subscription;
use crate::JoinErrToStr;

#[derive(Debug)]
pub struct Blobs {
    config: S3Config,
    blobs_actor_tx: mpsc::Sender<ToBlobsActor>,
    #[allow(dead_code)]
    actor_handle: Shared<MapErr<AbortOnDropHandle<()>, JoinErrToStr>>,
}

impl Blobs {
    pub fn new<S: Store>(config: S3Config, blobs_handler: BlobsHandler<Subscription, S>) -> Self {
        let (blobs_actor_tx, blobs_actor_rx) = mpsc::channel(256);
        let blobs_actor = BlobsActor::new(blobs_handler, blobs_actor_rx);
        let pool = LocalPoolHandle::new(1);

        let actor_handle = pool.spawn_pinned(|| async move {
            if let Err(err) = blobs_actor.run().await {
                error!("blobs actor failed: {err:?}");
            }
        });

        let actor_drop_handle = AbortOnDropHandle::new(actor_handle)
            .map_err(Box::new(|e: JoinError| e.to_string()) as JoinErrToStr)
            .shared();

        Self {
            config,
            blobs_actor_tx,
            actor_handle: actor_drop_handle,
        }
    }

    /// Import a file into the node's blob store on the file system and sync it with the internal
    /// MinIO database.
    // @TODO: We're currently using the filesystem blob store to calculate the bao-tree hashes
    // for the file. This is the only way to retrieve the blob hash right now. In the future we
    // want to do all of this inside of MinIO and skip loading the file onto the file-system
    // first.
    // Related issue: https://github.com/HIRO-MicroDataCenters-BV/rhio/issues/51
    pub async fn import_file(&self, file_path: PathBuf) -> Result<Hash> {
        let (reply, reply_rx) = oneshot::channel();
        self.blobs_actor_tx
            .send(ToBlobsActor::ImportFile { file_path, reply })
            .await?;
        let hash = reply_rx.await??;

        self.export_blob_minio(
            hash,
            self.config.region.clone(),
            self.config.endpoint.clone(),
            self.config.bucket_name.clone(),
        )
        .await?;

        Ok(hash)
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
            return Err(anyhow!("no minio credentials provided"));
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

        self.export_blob_minio(
            hash,
            self.config.region.clone(),
            self.config.endpoint.clone(),
            self.config.bucket_name.clone(),
        )
        .await?;

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
