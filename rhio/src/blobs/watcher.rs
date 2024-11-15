use std::{collections::HashSet, sync::Arc, time::Duration};

use iroh_blobs::Hash as BlobsHash;
use rhio_blobs::{
    paths::{META_SUFFIX, OUTBOARD_SUFFIX},
    S3Store,
};
use tokio::sync::{broadcast, RwLock};
use tracing::debug;

const POLL_FREQUENCY: Duration = Duration::from_secs(1);

const NO_PREFIX: String = String::new(); // Empty string.

/// Service watching the S3 buckets and p2p blob interface to inform us on newly detected objects.
#[derive(Clone, Debug)]
pub struct S3Watcher {
    event_tx: broadcast::Sender<S3WatcherEvent>,
    inner: Arc<RwLock<Inner>>,
}

#[derive(Clone, Debug)]
struct WatcherObject {
    pub size: u64,
    pub key: String,
    pub hash: Option<BlobsHash>,
}

impl PartialEq for WatcherObject {
    fn eq(&self, other: &Self) -> bool {
        self.size == other.size && self.key == other.key
    }
}

impl Eq for WatcherObject {}

impl std::hash::Hash for WatcherObject {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.size.hash(state);
        self.key.hash(state);
    }
}

#[derive(Debug)]
struct Inner {
    /// List of all S3 objects.
    s3_objects: HashSet<WatcherObject>,

    /// List of S3 objects which have been indexed and encoded for p2p blob sync.
    ///
    /// This can be either through an successful import (from our local S3 database) or from a
    /// successful download from remote peers.
    completed: HashSet<WatcherObject>,

    /// List of S3 objects which should be downloaded from remote peers but did not finish yet.
    incomplete: HashSet<WatcherObject>,
}

impl S3Watcher {
    pub fn new(store: S3Store) -> Self {
        let (event_tx, _) = broadcast::channel(64);

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
                    // 1. List of _all_ S3 objects in this bucket.
                    let mut maybe_to_be_imported = Vec::new();
                    match bucket.list(NO_PREFIX, Some("/".to_string())).await {
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

                                    let item = WatcherObject {
                                        size: object.size,
                                        key: object.key,
                                        hash: None,
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
                            // @TODO: Send critical error over channel.
                            return Err(err);
                        }
                    }

                    // 2. List of S3 objects which have been encoded / completed already and are
                    //    ready for p2p sync.
                    {
                        let list = store.complete_blobs().await;
                        let mut inner = inner.write().await;
                        for (hash, path, size) in list {
                            let is_new = inner.completed.insert(WatcherObject {
                                size,
                                key: path.data(),
                                hash: Some(hash),
                            });

                            // During the first iteration we're only establishing the initial state
                            // of completed items. For all further iterations we're sending events
                            // as soon as a new object was completed.
                            if is_new && !first_run {
                                debug!(key = %path.data(), size = %size, hash = %hash, "detected newly completed S3 object");
                                event_tx.send(S3WatcherEvent::BlobImportFinished(
                                    hash,
                                    size,
                                    path.data(),
                                ));
                            }
                        }

                        // Compare item from S3 database with completed ones, so we can identify
                        // which object has not yet been imported.
                        for object in maybe_to_be_imported {
                            if !inner.completed.contains(&object) {
                                debug!(key = %object.key, size = %object.size, "detected new S3 object, needs to be imported");
                                event_tx.send(S3WatcherEvent::DetectedS3Object(
                                    object.size,
                                    object.key,
                                ));
                            }
                        }
                    }

                    // 3. List of S3 objects which were started to be downloaded, but did not
                    //    finish yet.
                    {
                        let list = store.incomplete_blobs().await;
                        let mut inner = inner.write().await;
                        for (hash, path, size) in list {
                            let is_new = inner.incomplete.insert(WatcherObject {
                                size,
                                key: path.data(),
                                hash: Some(hash),
                            });
                            if is_new {
                                debug!(key = %path.data(), size = %size, hash = %hash, "detected incomplete S3 object, download needs to be resumed");
                                event_tx.send(S3WatcherEvent::DetectedIncompleteBlob(
                                    hash,
                                    size,
                                    path.data(),
                                ));
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

    pub fn subscribe(&mut self) -> broadcast::Receiver<S3WatcherEvent> {
        self.event_tx.subscribe()
    }
}

#[derive(Clone, Debug)]
pub enum S3WatcherEvent {
    DetectedS3Object(u64, String),
    BlobImportFinished(BlobsHash, u64, String),
    DetectedIncompleteBlob(BlobsHash, u64, String),
}
