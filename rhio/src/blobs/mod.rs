mod actor;
mod proxy;
pub mod watcher;

use std::collections::HashMap;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use p2panda_blobs::{Blobs as BlobsHandler, Config as BlobsConfig};
use proxy::BlobsActorProxy;
use rhio_blobs::{NotImportedObject, S3Store, SignedBlobInfo};
use s3::{Bucket, Region};
use tokio::sync::mpsc;
use watcher::S3Event;

use crate::config::Config;
use crate::topic::Query;

use crate::blobs::watcher::S3Watcher;
use s3::error::S3Error;

#[derive(Debug)]
pub struct Blobs {
    blobs: BlobsActorProxy,
    #[allow(dead_code)]
    watcher: S3Watcher,
}

impl Blobs {
    pub fn new(
        blob_store: S3Store,
        blobs_handler: BlobsHandler<Query, S3Store>,
        watcher_tx: mpsc::Sender<Result<S3Event, S3Error>>,
    ) -> Blobs {
        let blobs = BlobsActorProxy::new(blob_store.clone(), blobs_handler);
        let watcher = S3Watcher::new(blob_store, watcher_tx);
        Blobs { blobs, watcher }
    }

    /// Download a blob from the network.
    ///
    /// Attempt to download a blob from peers on the network and place it into the nodes MinIO
    /// bucket.
    pub async fn download(&self, blob: SignedBlobInfo) -> Result<()> {
        self.blobs.download(blob).await
    }

    /// Import an existing, local S3 object into the blob store, preparing it for p2p sync.
    pub async fn import_s3_object(&self, object: NotImportedObject) -> Result<()> {
        self.blobs.import_s3_object(object).await
    }

    pub async fn shutdown(&self) -> Result<()> {
        self.blobs.shutdown().await
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
    if let Some(s3_config) = config.s3.as_ref() {
        let mut buckets: HashMap<String, Bucket> = HashMap::new();

        let credentials = s3_config
            .credentials
            .as_ref()
            .ok_or(anyhow!("s3 credentials are not set"))
            .context("reading s3 credentials from config")?;
        let region: Region = Region::Custom {
            region: s3_config.region.clone(),
            endpoint: s3_config.endpoint.clone(),
        };

        // Merge all buckets mentioned in the regarding publish and subscribe config sections and
        // de-duplicate them. On this level we want them to be all handled by the same interface.
        if let Some(publish) = &config.publish {
            for bucket_name in &publish.s3_buckets {
                let bucket = Bucket::new(bucket_name, region.clone(), credentials.clone())?
                    .with_path_style();
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
    } else {
        S3Store::empty().await
    }
}
