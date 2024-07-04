use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use anyhow::Result;
use iroh_blobs::downloader::{DownloadRequest, Downloader};
use iroh_blobs::get::db::DownloadProgress;
use iroh_blobs::get::Stats;
use iroh_blobs::provider::AddProgress;
use iroh_blobs::store::{ImportMode, ImportProgress, Store};
use iroh_blobs::util::progress::{FlumeProgressSender, ProgressSender};
use iroh_blobs::{BlobFormat, HashAndFormat};
use iroh_net::{Endpoint, NodeAddr};
use serde::{Deserialize, Serialize};

/// Name used for logging when new node addresses are added from gossip.
const BLOB_DOWNLOAD_SOURCE_NAME: &str = "blob_download";

pub async fn download_queued(
    endpoint: Endpoint,
    downloader: &Downloader,
    hash_and_format: HashAndFormat,
    nodes: Vec<NodeAddr>,
    progress: FlumeProgressSender<DownloadProgress>,
) -> Result<Stats> {
    let mut node_ids = Vec::with_capacity(nodes.len());
    let mut any_added = false;
    // @TODO: Can we remove this?
    for node in nodes {
        node_ids.push(node.node_id);
        if !node.info.is_empty() {
            endpoint.add_node_addr_with_source(node, BLOB_DOWNLOAD_SOURCE_NAME)?;
            any_added = true;
        }
    }
    let can_download = !node_ids.is_empty() && (any_added || endpoint.discovery().is_some());
    anyhow::ensure!(can_download, "no way to reach a node for download");
    let req = DownloadRequest::new(hash_and_format, node_ids).progress_sender(progress);
    let handle = downloader.queue(req).await;
    let stats = handle.await?;
    Ok(stats)
}

pub async fn add_from_path<D: Store>(
    db: D,
    path: PathBuf,
    progress: flume::Sender<AddProgress>,
) -> anyhow::Result<()> {
    let progress = FlumeProgressSender::new(progress);
    let names = Arc::new(Mutex::new(BTreeMap::new()));
    // convert import progress to provide progress
    let import_progress = progress.clone().with_filter_map(move |x| match x {
        ImportProgress::Found { id, name } => {
            names.lock().unwrap().insert(id, name);
            None
        }
        ImportProgress::Size { id, size } => {
            let name = names.lock().unwrap().remove(&id)?;
            Some(AddProgress::Found { id, name, size })
        }
        ImportProgress::OutboardProgress { id, offset } => {
            Some(AddProgress::Progress { id, offset })
        }
        ImportProgress::OutboardDone { hash, id } => Some(AddProgress::Done { hash, id }),
        _ => None,
    });
    // Check that the path is absolute and exists.
    anyhow::ensure!(path.is_absolute(), "path must be absolute");
    anyhow::ensure!(
        path.exists(),
        "trying to add missing path: {}",
        path.display()
    );

    let import_mode = ImportMode::default();
    let (tag, _size) = db
        .import_file(path, import_mode, BlobFormat::Raw, import_progress)
        .await?;

    let hash_and_format = tag.inner();
    let HashAndFormat { hash, format } = *hash_and_format;
    let tag = db.create_tag(*hash_and_format).await?;
    progress
        .send(AddProgress::AllDone { hash, format, tag })
        .await?;
    Ok(())
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlobDownloadResponse(pub DownloadProgress);

#[derive(Debug, Serialize, Deserialize)]
pub struct BlobAddPathResponse(pub AddProgress);
