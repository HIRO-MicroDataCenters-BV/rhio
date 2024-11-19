use std::path::PathBuf;

use anyhow::{anyhow, Result};
use iroh_blobs::store::bao_tree::io::fsm::AsyncSliceReader;
use iroh_blobs::store::{MapEntry, Store};
use p2panda_blobs::{Blobs as BlobsHandler, DownloadBlobEvent, ImportBlobEvent};
use p2panda_core::Hash;
use rhio_blobs::{
    BlobHash, BucketName, NotImportedObject, ObjectKey, ObjectSize, Paths, S3Store, SignedBlobInfo,
};
use s3::creds::Credentials;
use s3::error::S3Error;
use s3::{Bucket, BucketConfiguration, Region};
use tokio::sync::{mpsc, oneshot};
use tokio_stream::StreamExt;
use tracing::{debug, error, span, Level};

use crate::blobs::watcher::S3Event;
use crate::topic::Query;

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
            bucket_name = %blob.bucket_name,
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
                }
                DownloadBlobEvent::Done => {
                    debug!(parent: &span, "finished downloading blob");
                }
            }
        }

        Ok(())
    }

    async fn shutdown(&mut self) -> Result<()> {
        Ok(())
    }
}
