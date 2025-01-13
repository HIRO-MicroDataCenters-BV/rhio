use crate::{
    metrics::{
        BLOBS_DOWNLOAD_TOTAL, LABEL_BLOB_MSG_TYPE_DONE, LABEL_BLOB_MSG_TYPE_ERROR,
        LABEL_LOCAL_BUCKET, LABEL_MSG_TYPE, LABEL_REMOTE_BUCKET,
    },
    topic::Query,
};
use anyhow::{anyhow, Result};
use axum_prometheus::metrics;
use p2panda_blobs::{Blobs as BlobsHandler, DownloadBlobEvent};
use p2panda_core::Hash;
use rhio_blobs::{NotImportedObject, S3Store, SignedBlobInfo};
use tokio::sync::{mpsc, oneshot};
use tokio_stream::StreamExt;
use tracing::{debug, error, span, Level};

#[allow(clippy::large_enum_variant)]
pub enum ToBlobsActor {
    ImportS3Object {
        object: NotImportedObject,
        reply: oneshot::Sender<Result<()>>,
    },
    DownloadBlob {
        blob: SignedBlobInfo,
        reply: oneshot::Sender<Result<()>>,
    },
    Shutdown {
        reply: oneshot::Sender<()>,
    },
}

pub struct BlobsActor {
    store: S3Store,
    inbox: mpsc::Receiver<ToBlobsActor>,
    blobs: BlobsHandler<Query, S3Store>,
}

impl BlobsActor {
    pub fn new(
        store: S3Store,
        blobs: BlobsHandler<Query, S3Store>,
        inbox: mpsc::Receiver<ToBlobsActor>,
    ) -> Self {
        Self {
            store,
            inbox,
            blobs,
        }
    }

    pub async fn run(mut self) -> Result<()> {
        // Take oneshot sender from external API awaited by `shutdown` call and fire it as soon as
        // shutdown completed.
        let shutdown_completed_signal = self.run_inner().await;
        if let Err(err) = self.shutdown().await {
            error!(?err, "error during shutdown");
        }

        match shutdown_completed_signal {
            Ok(reply_tx) => {
                reply_tx.send(()).ok();
                Ok(())
            }
            Err(err) => Err(err),
        }
    }

    async fn run_inner(&mut self) -> Result<oneshot::Sender<()>> {
        loop {
            tokio::select! {
                biased;
                Some(msg) = self.inbox.recv() => {
                    match msg {
                        ToBlobsActor::Shutdown { reply } => {
                            break Ok(reply);
                        }
                        msg => {
                            if let Err(err) = self.on_actor_message(msg).await {
                                break Err(err);
                            }
                        }
                    }
                },
                else => {
                    // Error occurred outside of actor and our select! loop got disabled. We exit
                    // here with an error which will probably be overriden by the external error
                    // which caused the problem in first hand.
                    break Err(anyhow!("all select! branches are disabled"));
                }
            }
        }
    }

    async fn on_actor_message(&mut self, msg: ToBlobsActor) -> Result<()> {
        match msg {
            ToBlobsActor::ImportS3Object { object, reply } => {
                let result = self.store.import_object(object).await;
                reply.send(result).ok();
            }
            ToBlobsActor::DownloadBlob { blob, reply } => {
                let result = self.on_download_blob(blob).await;
                reply.send(result).ok();
            }
            ToBlobsActor::Shutdown { .. } => {
                unreachable!("handled in run_inner");
            }
        }

        Ok(())
    }

    async fn on_download_blob(&mut self, blob: SignedBlobInfo) -> Result<()> {
        let span = span!(Level::DEBUG, "download",
            hash = %blob.hash,
            local_bucket_name = %blob.local_bucket_name,
            remote_bucket_name = %blob.remote_bucket_name,
            key = %blob.key,
            size = %blob.size,
        );
        debug!(parent: &span, "start downloading blob");

        self.store.blob_discovered(blob.clone()).await?;

        let mut stream = {
            let p2panda_hash = Hash::from_bytes(*blob.hash.as_bytes());
            Box::pin(self.blobs.download_blob(p2panda_hash).await)
        };

        while let Some(event) = stream.next().await {
            match event {
                DownloadBlobEvent::Abort(err) => {
                    error!(parent: &span, %err, "failed downloading blob");

                    BlobsActor::increment_blob_downloads(LABEL_BLOB_MSG_TYPE_ERROR, &blob);
                }
                DownloadBlobEvent::Done => {
                    debug!(parent: &span, "finished downloading blob");

                    BlobsActor::increment_blob_downloads(LABEL_BLOB_MSG_TYPE_DONE, &blob);
                }
            }
        }

        Ok(())
    }

    fn increment_blob_downloads(msg_type: &str, blob: &SignedBlobInfo) {
        metrics::counter!(
            BLOBS_DOWNLOAD_TOTAL,
            LABEL_MSG_TYPE => msg_type.to_owned(),
            LABEL_LOCAL_BUCKET => blob.local_bucket_name.to_owned(),
            LABEL_REMOTE_BUCKET => blob.remote_bucket_name.to_owned()
        )
        .increment(1);
    }

    async fn shutdown(&mut self) -> Result<()> {
        Ok(())
    }
}
