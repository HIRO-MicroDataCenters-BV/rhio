use p2panda_core::Hash;
use tracing::debug;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};

use crate::messages::FileSystemEvent;

pub enum FileSystemAction {
    DownloadAndExport { hash: Hash, path: PathBuf },
    Export { hash: Hash, path: PathBuf },
}

pub struct FileSystem {
    blobs: HashSet<Hash>,
    paths: HashMap<PathBuf, (Hash, u64)>,
}

type Timestamp = u64;

impl FileSystem {
    pub fn new() -> Self {
        Self {
            blobs: HashSet::new(),
            paths: HashMap::new(),
        }
    }

    pub fn process(
        &mut self,
        event: FileSystemEvent,
        timestamp: Timestamp,
    ) -> Vec<FileSystemAction> {
        self.on_event(event, timestamp)
    }

    pub fn file_announced(&self, hash: Hash, path: &Path) -> bool {
        match self.paths.get(path) {
            Some((blob, _)) => blob == &hash,
            None => false,
        }
    }

    fn on_event(&mut self, event: FileSystemEvent, timestamp: Timestamp) -> Vec<FileSystemAction> {
        let mut actions = Vec::new();

        // Handle messages
        match event {
            FileSystemEvent::Create(path, hash) => {
                // If the latest timestamp (with fallback to hash) at this path is greater than
                // the new timestamp, then ignore this event and return here already. This is LWW
                // logic in action.
                if let Some((latest_hash, latest_timestamp)) = self.paths.get(&path) {
                    if (timestamp, hash) < (*latest_timestamp, *latest_hash) {
                        debug!("ignore old file system event: {path:?} {hash}");
                        return actions;
                    }
                };

                // Insert the new path and hash.
                if let Some((current_hash, _)) =
                    self.paths.insert(PathBuf::from(&path), (hash, timestamp))
                {
                    // If there was already a different hash at that path then remove it from the
                    // blobs hash set if it isn't used at another path.
                    //
                    // @TODO: We could also delete it from the blob store at this point.
                    let hash_in_use = self.paths.values().any(|(hash, _)| hash == &current_hash);
                    if current_hash != hash && !hash_in_use {
                        debug!("remove unused blob hash: {current_hash}");
                        self.blobs.remove(&current_hash);
                    }
                };

                // Add hash to blobs set, if it wasn't already present we issue a download event.
                if self.blobs.insert(hash) {
                    actions.push(FileSystemAction::DownloadAndExport { hash, path });
                } else {
                    actions.push(FileSystemAction::Export { hash, path });
                }
            }
            FileSystemEvent::Modify => unimplemented!(),
            FileSystemEvent::Remove => unimplemented!(),
            FileSystemEvent::Snapshot(_) => unimplemented!(),
        }

        actions
    }
}
