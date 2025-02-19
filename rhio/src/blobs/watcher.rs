use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use rhio_blobs::{
    BlobHash, BucketName, CompletedBlob, IncompleteBlob, NotImportedObject, ObjectKey, ObjectSize,
    S3Store, SignedBlobInfo, META_SUFFIX, NO_PREFIX, OUTBOARD_SUFFIX,
};
use s3::error::S3Error;
use tokio::sync::{mpsc, RwLock};
use tokio::task::JoinHandle;
use tracing::{debug, warn};

const DEFAULT_POLL_INTERVAL: Duration = Duration::from_secs(60);

#[derive(Clone, Debug)]
pub struct S3WatcherOptions {
    pub poll_interval: Duration,
}

impl Default for S3WatcherOptions {
    fn default() -> Self {
        Self {
            poll_interval: DEFAULT_POLL_INTERVAL,
        }
    }
}

/// Service watching the S3 buckets and p2p blob interface to inform us on newly detected objects
/// and their import status.
#[derive(Clone, Debug)]
#[allow(dead_code)]
pub struct S3Watcher {
    inner: Arc<RwLock<Inner>>,
    handle: Arc<JoinHandle<Result<()>>>,
}

impl S3Watcher {
    pub fn new(
        store: S3Store,
        event_tx: mpsc::Sender<Result<S3Event, S3Error>>,
        options: S3WatcherOptions,
    ) -> Self {
        let inner = Arc::new(RwLock::new(Inner {
            s3_objects: HashSet::new(),
            completed: HashSet::new(),
            incomplete: HashSet::new(),
            event_tx: event_tx.clone(),
            store: store.clone(),
        }));

        let local_inner = inner.clone();

        let handle: JoinHandle<Result<(), _>> = tokio::spawn(async move {
            let mut first_run = true;

            loop {
                store.reload().await;
                {
                    let mut inner = local_inner.write().await;
                    for bucket in store.buckets() {
                        inner.detect_updates(bucket, first_run).await;
                    }
                    drop(inner);
                }

                tokio::time::sleep(options.poll_interval).await;

                first_run = false;
            }
        });

        Self {
            inner,
            handle: Arc::new(handle),
        }
    }
}

#[derive(Clone, Debug)]
struct WatchedObject {
    pub size: ObjectSize,
    pub local_bucket_name: BucketName,
    pub key: ObjectKey,
    #[allow(dead_code)]
    pub import_state: ImportState,
}

impl From<CompletedBlob> for WatchedObject {
    fn from(value: CompletedBlob) -> Self {
        match value {
            CompletedBlob::Unsigned(blob) => Self {
                size: blob.size,
                local_bucket_name: blob.local_bucket_name,
                key: blob.key,
                import_state: ImportState::Imported(blob.hash),
            },
            CompletedBlob::Signed(blob) => Self {
                size: blob.size,
                local_bucket_name: blob.local_bucket_name,
                key: blob.key,
                import_state: ImportState::Imported(blob.hash),
            },
        }
    }
}

impl From<SignedBlobInfo> for WatchedObject {
    fn from(blob: SignedBlobInfo) -> Self {
        Self {
            size: blob.size,
            local_bucket_name: blob.local_bucket_name,
            key: blob.key,
            import_state: ImportState::Imported(blob.hash),
        }
    }
}

impl Eq for WatchedObject {}

// Hash and compare size, key and bucket name but not import state.
impl PartialEq for WatchedObject {
    fn eq(&self, other: &Self) -> bool {
        self.size == other.size
            && self.key == other.key
            && self.local_bucket_name == other.local_bucket_name
    }
}

impl std::hash::Hash for WatchedObject {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.local_bucket_name.hash(state);
        self.key.hash(state);
        self.size.hash(state);
    }
}

#[derive(Clone, Debug)]
enum ImportState {
    NotImported,
    #[allow(dead_code)]
    Imported(BlobHash),
}

#[derive(Debug)]
struct Inner {
    /// List of all S3 objects.
    s3_objects: HashSet<WatchedObject>,

    /// List of S3 objects which have been indexed and encoded for p2p blob sync.
    ///
    /// This can be either through an successful import (from our local S3 buckets) or from a
    /// successful download from remote peers.
    completed: HashSet<WatchedObject>,

    /// List of S3 objects which should be downloaded from remote peers but did not finish yet.
    incomplete: HashSet<WatchedObject>,

    /// Blob store
    store: S3Store,

    /// Blob Update Events
    event_tx: mpsc::Sender<Result<S3Event, S3Error>>,
}

impl Inner {
    async fn detect_updates(&mut self, bucket: &s3::Bucket, first_run: bool) {
        // 1 List of _all_ S3 objects in this bucket.
        let new_watched = self.get_new_blobs(bucket).await;

        // 2 List of S3 objects which have been encoded / completed already and are
        //    ready for p2p sync.
        self.complete_blobs(first_run).await;
        self.announce_detected_objects(new_watched).await;

        // 3 List of S3 objects which were started to be downloaded, but did not
        //    finish yet.
        {
            // Only run this when the app starts.
            if first_run {
                self.detecting_incomplete_blobs().await;
            }
        }
    }

    async fn get_new_blobs(&mut self, bucket: &s3::Bucket) -> Vec<WatchedObject> {
        let mut maybe_to_be_imported = Vec::new();
        let maybe_bucket_list = bucket.list(NO_PREFIX, None).await;
        match maybe_bucket_list {
            Ok(pages) => {
                for page in pages {
                    for object in page.contents {
                        // Filter out objects in database which are related to rhio
                        // blob syncing. They live right next to the actual blobs in
                        // the same S3 bucket.
                        if object.key.ends_with(META_SUFFIX)
                            || object.key.ends_with(OUTBOARD_SUFFIX)
                        {
                            continue;
                        }

                        let watched = WatchedObject {
                            size: object.size,
                            local_bucket_name: bucket.name(),
                            key: object.key,
                            import_state: ImportState::NotImported,
                        };

                        // If object was observed for the first time, earmark it so we
                        // can check later if it's download or import was already
                        // completed.
                        let is_new = self.s3_objects.insert(watched.clone());
                        if is_new {
                            maybe_to_be_imported.push(watched);
                        }
                    }
                }
            }
            Err(err) => {
                warn!("failed to list bucket objects: {:?}, skipping...", err);
            }
        }
        maybe_to_be_imported
    }

    async fn complete_blobs(&mut self, first_run: bool) {
        let list = self.store.complete_blobs();

        for completed_blob in list {
            let watched = WatchedObject::from(completed_blob.clone());

            // Remove object from "incomplete" list and add it to "completed".
            self.incomplete.remove(&watched);
            let is_new = self.completed.insert(watched.clone());

            // During the first iteration we're only establishing the initial state
            // of completed items. For all further iterations we're sending events
            // as soon as a new object was completed.
            if is_new && !first_run {
                debug!(
                    key = %completed_blob.key(),
                    size = %completed_blob.size(),
                    hash = %completed_blob.hash(),
                    local_bucket_name = %completed_blob.local_bucket_name(),
                    "detected finished blob import"
                );

                if self
                    .event_tx
                    .send(Ok(S3Event::BlobImportFinished(completed_blob)))
                    .await
                    .is_err()
                {
                    warn!("failed to send S3Event::BlobImportFinished");
                    self.completed.remove(&watched);
                    self.incomplete.insert(watched);
                }
            }
        }
    }

    async fn announce_detected_objects(&mut self, new_watched: Vec<WatchedObject>) {
        // Compare item from S3 database with completed ones, so we can identify
        // which object has not yet been imported.
        for object in new_watched {
            if !self.completed.contains(&object) {
                debug!(
                    key = %object.key,
                    size = %object.size,
                    local_bucket_name = %object.local_bucket_name,
                    "detected new S3 object to be imported"
                );

                if self
                    .event_tx
                    .send(Ok(S3Event::DetectedS3Object(NotImportedObject {
                        local_bucket_name: object.local_bucket_name.to_owned(),
                        key: object.key.to_owned(),
                        size: object.size,
                    })))
                    .await
                    .is_err()
                {
                    warn!(
                        key = %object.key,
                        size = %object.size,
                        local_bucket_name = %object.local_bucket_name,
                        "failed to send S3Event::DetectedS3Object event"
                    );
                }
            }
        }
    }

    async fn detecting_incomplete_blobs(&mut self) {
        let incomplete_blobs = self.store.incomplete_blobs();
        for incomplete_blob in incomplete_blobs {
            let watched = WatchedObject::from(incomplete_blob.clone());
            let is_new = self.incomplete.insert(watched.clone());

            if is_new {
                debug!(
                    key = %incomplete_blob.key,
                    size = %incomplete_blob.size,
                    hash = %incomplete_blob.hash,
                    local_bucket_name = %incomplete_blob.local_bucket_name,
                    remote_bucket_name = %incomplete_blob.remote_bucket_name,
                    "detected incomplete blob download"
                );

                if self
                    .event_tx
                    .send(Ok(S3Event::DetectedIncompleteBlob(incomplete_blob)))
                    .await
                    .is_err()
                {
                    warn!("failed to send S3Event::DetectedIncompleteBlob");
                    self.incomplete.remove(&watched);
                }
            }
        }
    }
}

#[derive(Clone, Debug)]
pub enum S3Event {
    DetectedS3Object(NotImportedObject),
    BlobImportFinished(CompletedBlob),
    DetectedIncompleteBlob(IncompleteBlob),
}

#[cfg(test)]
pub mod tests {

    use super::*;
    use crate::{
        blobs::store_from_config,
        tests::{
            configuration::{
                configure_publish_bucket, generate_nats_config, generate_rhio_config,
                generate_s3_config, new_s3_server,
            },
            utils::wait_for_condition,
        },
    };
    use anyhow::Context;
    use rhio_blobs::BucketState;
    use s3_server::FakeS3Server;
    use tokio::runtime::Builder;
    use tracing::info;

    #[test]
    pub fn test_bucket_watcher_resilience() -> Result<()> {
        let TestSetup {
            s3_source,
            store,
            watcher: _watcher,
        } = create_test_setup()?;

        wait_for_condition(Duration::from_secs(10), || {
            if let Some(status) = store.status("source-bucket".into()) {
                if status.state == BucketState::NotInitialized {
                    return Ok(true);
                }
            }
            Ok(false)
        })?;

        s3_source.create_bucket("source-bucket")?;

        wait_for_condition(Duration::from_secs(10), || {
            if let Some(status) = store.status("source-bucket".into()) {
                if status.state == BucketState::Active {
                    return Ok(true);
                }
            }
            Ok(false)
        })?;

        s3_source.delete_bucket("source-bucket")?;

        wait_for_condition(Duration::from_secs(10), || {
            if let Some(status) = store.status("source-bucket".into()) {
                if status.state == BucketState::Inactive {
                    return Ok(true);
                }
            }
            Ok(false)
        })?;

        Ok(())
    }

    struct TestSetup {
        s3_source: FakeS3Server,
        store: S3Store,
        watcher: S3Watcher,
    }

    fn create_test_setup() -> Result<TestSetup> {
        let nats_source_config = generate_nats_config();
        let s3_source_config = generate_s3_config();
        info!("s3 source config {:?}", s3_source_config);

        let mut rhio_source_config =
            generate_rhio_config(&nats_source_config, &Some(s3_source_config.clone()));

        configure_publish_bucket(&mut rhio_source_config, "source-bucket");

        let test_runtime = Arc::new(
            Builder::new_multi_thread()
                .enable_io()
                .enable_time()
                .thread_name("test-runtime")
                .worker_threads(3)
                .build()
                .expect("test tokio runtime"),
        );

        let s3_source = new_s3_server(&s3_source_config, test_runtime.clone())?;

        let store_and_watcher: Result<(S3Store, S3Watcher), anyhow::Error> =
            test_runtime.block_on(async {
                let store = store_from_config(&rhio_source_config).context("store from config")?;
                store.reload().await;
                let (tx, _rx) = mpsc::channel(1);
                let options = S3WatcherOptions {
                    poll_interval: Duration::from_millis(100),
                };
                let watcher = S3Watcher::new(store.clone(), tx, options);
                Ok((store, watcher))
            });

        let (store, watcher) = store_and_watcher?;

        Ok(TestSetup {
            s3_source,
            store,
            watcher,
        })
    }
}
