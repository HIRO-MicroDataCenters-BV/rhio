// SPDX-License-Identifier: AGPL-3.0-or-later

use anyhow::{ensure, Result};
use futures_lite::StreamExt;
use iroh_blobs::downloader::{DownloadRequest, Downloader};
use iroh_blobs::get::db::DownloadProgress;
use iroh_blobs::get::Stats;
use iroh_blobs::util::progress::{FlumeProgressSender, ProgressSender};
use iroh_blobs::{BlobFormat, Hash as IrohHash, HashAndFormat};
use p2panda_core::Hash;
use p2panda_net::Network;
use serde::{Deserialize, Serialize};
use tokio_stream::Stream;
use tokio_util::task::LocalPoolHandle;

pub async fn download_blob(
    network: Network,
    downloader: Downloader,
    pool_handle: LocalPoolHandle,
    hash: Hash,
) -> impl Stream<Item = DownloadBlobEvent> {
    let (sender, receiver) = flume::bounded(1024);
    let progress = FlumeProgressSender::new(sender);
    let format = BlobFormat::Raw;
    let hash = IrohHash::new(hash.as_bytes());
    let hash_and_format = HashAndFormat { hash, format };

    pool_handle.spawn_pinned(move || async move {
        match download_queued(network, &downloader, hash_and_format, progress.clone()).await {
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
    network: Network,
    downloader: &Downloader,
    hash_and_format: HashAndFormat,
    progress: FlumeProgressSender<DownloadProgress>,
) -> Result<Stats> {
    let node_addrs = network.known_peers().await?;

    let can_download = !node_addrs.is_empty() && network.endpoint().discovery().is_some();
    ensure!(can_download, "no way to reach a node for download");

    let req = DownloadRequest::new(hash_and_format, node_addrs).progress_sender(progress);
    let handle = downloader.queue(req).await;

    let stats = handle.await?;
    Ok(stats)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadBlobEvent(pub DownloadProgress);
