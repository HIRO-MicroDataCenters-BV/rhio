// SPDX-License-Identifier: AGPL-3.0-or-later

use anyhow::Result;
use futures_lite::StreamExt;
use iroh_blobs::downloader::{DownloadRequest, Downloader};
use iroh_blobs::get::db::DownloadProgress;
use iroh_blobs::get::Stats;
use iroh_blobs::util::progress::{FlumeProgressSender, ProgressSender};
use iroh_blobs::{BlobFormat, Hash as IrohHash, HashAndFormat};
use iroh_net::{Endpoint, NodeAddr};
use p2panda_core::Hash;
use serde::{Deserialize, Serialize};
use tokio_stream::Stream;
use tokio_util::task::LocalPoolHandle;

/// Name used for logging when new node addresses are added from gossip.
const BLOB_DOWNLOAD_SOURCE_NAME: &str = "blob_download";

pub async fn download_blob(
    endpoint: Endpoint,
    downloader: Downloader,
    pool_handle: LocalPoolHandle,
    node_addrs: Vec<NodeAddr>,
    hash: Hash,
) -> impl Stream<Item = DownloadBlobEvent> {
    let (sender, receiver) = flume::bounded(1024);
    let progress = FlumeProgressSender::new(sender);
    let format = BlobFormat::Raw;
    let hash = IrohHash::new(hash.as_bytes());
    let hash_and_format = HashAndFormat { hash, format };

    pool_handle.spawn_pinned(move || async move {
        match download_queued(
            endpoint,
            &downloader,
            hash_and_format,
            node_addrs,
            progress.clone(),
        )
        .await
        {
            Ok(stats) => {
                progress.send(DownloadProgress::AllDone(stats)).await.ok();
            }
            Err(err) => {
                progress
                    .send(DownloadProgress::Abort(err.into()))
                    .await
                    .ok();
            }
        }
    });

    receiver.into_stream().map(DownloadBlobEvent)
}

async fn download_queued(
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadBlobEvent(pub DownloadProgress);
