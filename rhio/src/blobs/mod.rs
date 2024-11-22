mod actor;
pub mod watcher;

use std::collections::HashMap;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use futures_util::future::{MapErr, Shared};
use futures_util::{FutureExt, TryFutureExt};
use p2panda_blobs::{Blobs as BlobsHandler, Config as BlobsConfig};
use rhio_blobs::{NotImportedObject, S3Store, SignedBlobInfo};
use s3::{Bucket, Region};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinError;
use tokio_util::task::{AbortOnDropHandle, LocalPoolHandle};
use tracing::error;

use crate::blobs::actor::{BlobsActor, ToBlobsActor};
use crate::config::Config;
use crate::topic::Query;
use crate::JoinErrToStr;

#[derive(Debug)]
pub struct Blobs {
    blobs_actor_tx: mpsc::Sender<ToBlobsActor>,
    #[allow(dead_code)]
    actor_handle: Shared<MapErr<AbortOnDropHandle<()>, JoinErrToStr>>,
}

impl Blobs {
    pub fn new(blob_store: S3Store, blobs_handler: BlobsHandler<Query, S3Store>) -> Self {
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

pub fn blobs_config() -> BlobsConfig {
    BlobsConfig {
        // Max. number of nodes we connect to for blob download.
        //
        // @TODO: This needs to be set to 1 as we're currently not allowed to write bytes
        // out-of-order. See comment in `s3_file.rs` in `rhio-blobs` for more details.
        max_concurrent_dials_per_hash: 1,
        initial_retry_delay: Duration::from_secs(10),
        ..Default::default()
    }
}

/// Initiates and returns a blob store for S3 buckets based on the rhio config.
///
/// This method fails when we couldn't connect to the S3 buckets due to invalid configuration
/// values, authentication or connection errors.
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
        for remote_bucket in &subscribe.s3_buckets {
            let bucket = Bucket::new(
                &remote_bucket.local_bucket_name,
                region.clone(),
                credentials.clone(),
            )?
            .with_path_style();
            buckets.insert(remote_bucket.local_bucket_name.clone(), *bucket);
        }
    }

    let buckets: Vec<Bucket> = buckets.values().cloned().collect();
    let store = S3Store::new(buckets)
        .await
        .context("could not initialize s3 interface")?;

    Ok(store)
}
