use p2panda_core::Hash;
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use tracing::error;

use crate::events::Event;

pub enum FileSystemAction {
    DownloadAndExport { hash: Hash, path: PathBuf },
    None,
}

pub struct FileSystem {
    blobs: HashSet<Hash>,
    paths: HashMap<PathBuf, Hash>,
}

impl FileSystem {
    pub fn new() -> Self {
        Self {
            blobs: HashSet::new(),
            paths: HashMap::new(),
        }
    }

    pub fn process(&mut self, event: Event) -> Vec<FileSystemAction> {
        self.on_event(event)
    }

    pub fn file_announced(&self, hash: Hash, path: &Path) -> bool {
        match self.paths.get(path) {
            Some(blob) => blob == &hash,
            None => false,
        }
    }

    pub fn file_exists(&self, path: &PathBuf) -> bool {
        self.paths.contains_key(path)
    }

    fn on_event(&mut self, event: Event) -> Vec<FileSystemAction> {
        let mut actions = Vec::new();

        // Handle messages
        match event {
            Event::Create(path, hash) => {
                // Add path and hash to blobs map.
                let path = PathBuf::from(path);
                self.paths.insert(PathBuf::from(&path), hash);

                // Add hash to blobs set, if it wasn't already present we issue a download event.
                if self.blobs.insert(hash) {
                    actions.push(FileSystemAction::DownloadAndExport { hash, path });
                } else {
                    error!("Failed to process `Create` event: file already exists");
                    actions.push(FileSystemAction::None);
                }
            }
            Event::Modify => unimplemented!(),
            Event::Remove => unimplemented!(),
            Event::Snapshot(_) => unimplemented!(),
        }

        actions
    }
}
