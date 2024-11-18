use std::path::PathBuf;

use anyhow::{anyhow, Result};
use iroh_blobs::store::bao_tree::io::fsm::AsyncSliceReader;
use iroh_blobs::store::{MapEntry, Store};
use p2panda_blobs::{Blobs as BlobsHandler, DownloadBlobEvent, ImportBlobEvent};
use p2panda_core::Hash;
use rhio_blobs::{BlobHash, BucketName, ObjectKey, ObjectSize, Paths, S3Store};
use s3::creds::Credentials;
use s3::error::S3Error;
use s3::{Bucket, BucketConfiguration, Region};
use tokio::sync::{mpsc, oneshot};
use tokio_stream::StreamExt;
use tracing::{debug, error};

use crate::blobs::watcher::S3Event;
use crate::topic::Query;

#[allow(clippy::large_enum_variant)]
pub enum ToBlobsActor {
    ImportS3Object {
        bucket_name: String,
        key: String,
        size: u64,
        reply: oneshot::Sender<Result<()>>,
    },
    DownloadBlob {
        hash: BlobHash,
        bucket_name: BucketName,
        key: ObjectKey,
        size: ObjectSize,
        reply: oneshot::Sender<Result<()>>,
    },
    CompleteBlobs {
        reply: oneshot::Sender<Vec<(BlobHash, BucketName, Paths, ObjectSize)>>,
    },
    IncompleteBlobs {
        reply: oneshot::Sender<Vec<(BlobHash, BucketName, Paths, ObjectSize)>>,
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
            ToBlobsActor::ImportS3Object {
                bucket_name,
                key,
                size,
                reply,
            } => {
                let result = self.store.import_object(&bucket_name, key, size).await;
                reply.send(result).ok();
            }
            ToBlobsActor::DownloadBlob {
                hash,
                bucket_name,
                key,
                size,
                reply,
            } => {
                let result = self.on_download_blob(hash, bucket_name, key, size).await;
                reply.send(result).ok();
            }
            ToBlobsActor::CompleteBlobs { reply } => {
                let result = self.store.complete_blobs().await;
                reply.send(result).ok();
            }
            ToBlobsActor::IncompleteBlobs { reply } => {
                let result = self.store.incomplete_blobs().await;
                reply.send(result).ok();
            }
            ToBlobsActor::Shutdown { .. } => {
                unreachable!("handled in run_inner");
            }
        }

        Ok(())
    }

    async fn on_download_blob(
        &mut self,
        hash: BlobHash,
        bucket_name: BucketName,
        key: ObjectKey,
        size: ObjectSize,
    ) -> Result<()> {
        self.store
            .blob_discovered(hash, &bucket_name, key.clone(), size)
            .await?;

        let mut stream = {
            let p2panda_hash = Hash::from_bytes(*hash.as_bytes());
            Box::pin(self.blobs.download_blob(p2panda_hash).await)
        };

        while let Some(event) = stream.next().await {
            match event {
                DownloadBlobEvent::Abort(err) => {
                    error!(%err, "failed downloading blob");
                }
                DownloadBlobEvent::Done => {
                    debug!(%hash, %bucket_name, %key, %size, "finished downloading blob");
                }
            }
        }

        Ok(())
    }

    async fn shutdown(&mut self) -> Result<()> {
        Ok(())
    }
}
