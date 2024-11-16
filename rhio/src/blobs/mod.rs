mod actor;
pub mod watcher;

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;

use anyhow::{anyhow, Context, Result};
use async_nats::jetstream::publish;
use futures_util::future::{MapErr, Shared};
use futures_util::{FutureExt, TryFutureExt};
use iroh_blobs::store::Store;
use p2panda_blobs::Blobs as BlobsHandler;
use p2panda_core::Hash;
use rhio_blobs::{BlobHash, BucketName, ObjectKey, ObjectSize, Paths, S3Store};
use rhio_core::ScopedBucket;
use s3::creds::Credentials;
use s3::{Bucket, Region};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinError;
use tokio_util::task::{AbortOnDropHandle, LocalPoolHandle};
use tracing::error;

use crate::blobs::actor::{BlobsActor, ToBlobsActor};
use crate::blobs::watcher::S3Watcher;
use crate::config::{Config, S3Config};
use crate::topic::Query;
use crate::JoinErrToStr;

#[derive(Debug)]
pub struct Blobs {
    config: S3Config,
    blobs_actor_tx: mpsc::Sender<ToBlobsActor>,
    #[allow(dead_code)]
    actor_handle: Shared<MapErr<AbortOnDropHandle<()>, JoinErrToStr>>,
}

impl Blobs {
    pub fn new(
        config: S3Config,
        blob_store: S3Store,
        blobs_handler: BlobsHandler<Query, S3Store>,
    ) -> Self {
        let (blobs_actor_tx, blobs_actor_rx) = mpsc::channel(256);
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
            config,
            blobs_actor_tx,
            actor_handle: actor_drop_handle,
        }
    }

    /// Query the blob store for all complete blobs.
    pub async fn complete_blobs(&self) -> Result<Vec<(BlobHash, BucketName, Paths, ObjectSize)>> {
        let (reply, reply_rx) = oneshot::channel();
        self.blobs_actor_tx
            .send(ToBlobsActor::CompleteBlobs { reply })
            .await?;
        let result = reply_rx.await?;
        Ok(result)
    }

    /// Query the blob store for all incomplete blobs.
    pub async fn incomplete_blobs(&self) -> Result<Vec<(BlobHash, BucketName, Paths, ObjectSize)>> {
        let (reply, reply_rx) = oneshot::channel();
        self.blobs_actor_tx
            .send(ToBlobsActor::IncompleteBlobs { reply })
            .await?;
        let result = reply_rx.await?;
        Ok(result)
    }

    /// Download a blob from the network.
    ///
    /// Attempt to download a blob from peers on the network and place it into the nodes MinIO
    /// bucket.
    pub async fn download(
        &self,
        hash: BlobHash,
        bucket: ScopedBucket,
        key: ObjectKey,
        size: ObjectSize,
    ) -> Result<()> {
        let (reply, reply_rx) = oneshot::channel();
        self.blobs_actor_tx
            .send(ToBlobsActor::DownloadBlob {
                hash,
                bucket,
                key,
                size,
                reply,
            })
            .await?;
        let result = reply_rx.await?;
        result?;
        Ok(())
    }

    /// Import an existing, local S3 object into the blob store, preparing it for p2p sync.
    pub async fn import_s3_object(
        &self,
        bucket_name: String,
        key: String,
        size: u64,
    ) -> Result<()> {
        let (reply, reply_rx) = oneshot::channel();
        self.blobs_actor_tx
            .send(ToBlobsActor::ImportS3Object {
                bucket_name,
                key,
                size,
                reply,
            })
            .await?;
        let result = reply_rx.await?;
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

/// Initiates and returns a blob store handler for S3 backends based on the rhio config.
pub async fn store_from_config(config: &Config) -> Result<S3Store> {
    let mut buckets: HashMap<String, Bucket> = HashMap::new();

    let credentials = config
        .s3
        .credentials
        .as_ref()
        .ok_or(anyhow!("s3 credentials are not set"))
        .context("reading s3 credentials from config")?;
    let region: Region = Region::Custom {
        region: config.s3.region.clone(),
        endpoint: config.s3.endpoint.clone(),
    };

    // Merge all buckets mentioned in the regarding publish and subscribe config sections and
    // de-duplicate them. On this level we want them to be all handled by the same interface.
    if let Some(publish) = &config.publish {
        for bucket_name in &publish.s3_buckets {
            let bucket =
                Bucket::new(bucket_name, region.clone(), credentials.clone())?.with_path_style();
            buckets.insert(bucket_name.clone(), *bucket);
        }
    }

    if let Some(subscribe) = &config.subscribe {
        for scoped_bucket in &subscribe.s3_buckets {
            let bucket_name = scoped_bucket.bucket_name();
            let bucket =
                Bucket::new(&bucket_name, region.clone(), credentials.clone())?.with_path_style();
            buckets.insert(bucket_name, *bucket);
        }
    }

    let buckets: Vec<Bucket> = buckets.values().cloned().collect();
    let store = S3Store::new(buckets)
        .await
        .context("could not initialize s3 interface")?;

    Ok(store)
}
