use anyhow::Result;
use futures_util::future::{MapErr, Shared};
use futures_util::{FutureExt, TryFutureExt};
use p2panda_blobs::Blobs as BlobsHandler;
use rhio_blobs::{NotImportedObject, S3Store, SignedBlobInfo};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinError;
use tokio_util::task::{AbortOnDropHandle, LocalPoolHandle};
use tracing::error;

use crate::JoinErrToStr;
use crate::blobs::actor::{BlobsActor, ToBlobsActor};
use crate::topic::Query;

#[derive(Debug)]
pub struct BlobsActorProxy {
    blobs_actor_tx: mpsc::Sender<ToBlobsActor>,
    #[allow(dead_code)]
    actor_handle: Shared<MapErr<AbortOnDropHandle<()>, JoinErrToStr>>,
}

impl BlobsActorProxy {
    pub fn new(blob_store: S3Store, blobs_handler: BlobsHandler<Query, S3Store>) -> Self {
        let (blobs_actor_tx, blobs_actor_rx) = mpsc::channel(512);
        let blobs_actor = BlobsActor::new(blob_store.clone(), blobs_handler, blobs_actor_rx);

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
            blobs_actor_tx,
            actor_handle: actor_drop_handle,
        }
    }

    /// Download a blob from the network.
    ///
    /// Attempt to download a blob from peers on the network and place it into the nodes MinIO
    /// bucket.
    pub async fn download(&self, blob: SignedBlobInfo) -> Result<()> {
        let (reply, reply_rx) = oneshot::channel();
        self.blobs_actor_tx
            .send(ToBlobsActor::DownloadBlob { blob, reply })
            .await?;
        let result = reply_rx.await?;
        result?;
        Ok(())
    }

    /// Import an existing, local S3 object into the blob store, preparing it for p2p sync.
    pub async fn import_s3_object(&self, object: NotImportedObject) -> Result<()> {
        let (reply, reply_rx) = oneshot::channel();
        self.blobs_actor_tx
            .send(ToBlobsActor::ImportS3Object { object, reply })
            .await?;
        reply_rx.await??;
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
