use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use rhio_blobs::{
    BlobHash, BucketName, ObjectKey, ObjectSize, S3Store, META_SUFFIX, NO_PREFIX, OUTBOARD_SUFFIX,
};
use s3::error::S3Error;
use tokio::sync::{broadcast, mpsc, RwLock};
use tokio_util::task::LocalPoolHandle;
use tracing::debug;

const POLL_FREQUENCY: Duration = Duration::from_secs(1);

/// Service watching the S3 buckets and p2p blob interface to inform us on newly detected objects
/// and their import status.
#[derive(Clone, Debug)]
pub struct S3Watcher {
    event_tx: mpsc::Sender<Result<S3Event, S3Error>>,
    inner: Arc<RwLock<Inner>>,
}

#[derive(Clone, Debug)]
struct WatchedObject {
    pub size: ObjectSize,
    pub key: ObjectKey,
    pub import_state: ImportState,
}

impl Eq for WatchedObject {}

impl PartialEq for WatchedObject {
    fn eq(&self, other: &Self) -> bool {
        self.size == other.size && self.key == other.key
    }
}

impl std::hash::Hash for WatchedObject {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.size.hash(state);
        self.key.hash(state);
    }
}

#[derive(Clone, Debug)]
enum ImportState {
    NotImported,
    Imported(BlobHash),
}

#[derive(Debug)]
struct Inner {
    /// List of all S3 objects.
    s3_objects: HashSet<WatchedObject>,

    /// List of S3 objects which have been indexed and encoded for p2p blob sync.
    ///
    /// This can be either through an successful import (from our local S3 database) or from a
    /// successful download from remote peers.
    completed: HashSet<WatchedObject>,

    /// List of S3 objects which should be downloaded from remote peers but did not finish yet.
    incomplete: HashSet<WatchedObject>,
}

impl S3Watcher {
    pub fn new(store: S3Store, event_tx: mpsc::Sender<Result<S3Event, S3Error>>) -> Self {
        let inner = Arc::new(RwLock::new(Inner {
            s3_objects: HashSet::new(),
            completed: HashSet::new(),
            incomplete: HashSet::new(),
        }));

        let watcher = Self {
            event_tx: event_tx.clone(),
            inner: inner.clone(),
        };

        tokio::spawn(async move {
            let mut first_run = true;

            loop {
                for bucket in store.buckets() {
                    let bucket_name = bucket.name();

                    // 1. List of _all_ S3 objects in this bucket.
                    let mut maybe_to_be_imported = Vec::new();
                    match bucket.list(NO_PREFIX, None).await {
                        Ok(pages) => {
                            let mut inner = inner.write().await;
                            for page in pages {
                                for object in page.contents {
                                    // Filter out objects in database which are related to rhio
                                    // blob syncing. They life right next to the actual blobs in
                                    // the same S3 bucket.
                                    if object.key.ends_with(META_SUFFIX)
                                        || object.key.ends_with(OUTBOARD_SUFFIX)
                                    {
                                        continue;
                                    }

                                    let item = WatchedObject {
                                        size: object.size,
                                        key: object.key,
                                        import_state: ImportState::NotImported,
                                    };

                                    // If object was observed for the first time, earmark it so we
                                    // can check later if it's download or import was already
                                    // completed.
                                    let is_new = inner.s3_objects.insert(item.clone());
                                    if is_new {
                                        maybe_to_be_imported.push(item);
                                    }
                                }
                            }
                        }
                        Err(err) => {
                            event_tx.send(Err(err)).await.expect("send event");
                            return Err(());
                        }
                    }

                    // 2. List of S3 objects which have been encoded / completed already and are
                    //    ready for p2p sync.
                    {
                        let list = store.complete_blobs().await;
                        let mut inner = inner.write().await;
                        for (hash, _, path, size) in list {
                            let is_new = inner.completed.insert(WatchedObject {
                                size,
                                key: path.data(),
                                import_state: ImportState::Imported(hash),
                            });

                            // During the first iteration we're only establishing the initial state
                            // of completed items. For all further iterations we're sending events
                            // as soon as a new object was completed.
                            if is_new && !first_run {
                                debug!(key = %path.data(), size = %size, hash = %hash, "detected finished blob import");
                                if event_tx
                                    .send(Ok(S3Event::BlobImportFinished(
                                        hash,
                                        bucket_name.clone(),
                                        path.data(),
                                        size,
                                    )))
                                    .await
                                    .is_err()
                                {
                                    return Err(());
                                }
                            }
                        }

                        // Compare item from S3 database with completed ones, so we can identify
                        // which object has not yet been imported.
                        for object in maybe_to_be_imported {
                            if !inner.completed.contains(&object) {
                                debug!(key = %object.key, size = %object.size, "detected new S3 object to be imported");
                                if event_tx
                                    .send(Ok(S3Event::DetectedS3Object(
                                        bucket_name.clone(),
                                        object.key,
                                        object.size,
                                    )))
                                    .await
                                    .is_err()
                                {
                                    return Err(());
                                }
                            }
                        }
                    }

                    // 3. List of S3 objects which were started to be downloaded, but did not
                    //    finish yet.
                    {
                        let list = store.incomplete_blobs().await;
                        let mut inner = inner.write().await;
                        for (hash, _, path, size) in list {
                            let is_new = inner.incomplete.insert(WatchedObject {
                                size,
                                key: path.data(),
                                import_state: ImportState::Imported(hash),
                            });
                            if is_new {
                                debug!(key = %path.data(), size = %size, hash = %hash, "detected incomplete blob download");
                                if event_tx
                                    .send(Ok(S3Event::DetectedIncompleteBlob(
                                        hash,
                                        bucket_name.clone(),
                                        path.data(),
                                        size,
                                    )))
                                    .await
                                    .is_err()
                                {
                                    return Err(());
                                }
                            }
                        }
                    }
                }

                tokio::time::sleep(POLL_FREQUENCY).await;

                first_run = false;
            }

            Ok(())
        });

        watcher
    }
}

#[derive(Clone, Debug)]
pub enum S3Event {
    DetectedS3Object(BucketName, ObjectKey, ObjectSize),
    BlobImportFinished(BlobHash, BucketName, ObjectKey, ObjectSize),
    DetectedIncompleteBlob(BlobHash, BucketName, ObjectKey, ObjectSize),
}
