// SPDX-License-Identifier: AGPL-3.0-or-later

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use anyhow::Result;
use futures_lite::StreamExt;
use futures_util::Stream;
use iroh_blobs::provider::AddProgress;
use iroh_blobs::store::{ImportMode, ImportProgress, Store};
use iroh_blobs::util::progress::{FlumeProgressSender, ProgressSender};
use iroh_blobs::{BlobFormat, HashAndFormat};
use serde::{Deserialize, Serialize};
use tokio_util::task::LocalPoolHandle;

pub async fn import_blob<S: Store>(
    store: S,
    pool_handle: LocalPoolHandle,
    path: PathBuf,
) -> impl Stream<Item = ImportBlobEvent> {
    let (sender, receiver) = flume::bounded(32);

    {
        let sender = sender.clone();
        pool_handle.spawn_pinned(|| async move {
            if let Err(e) = add_from_path(store, path, sender.clone()).await {
                sender.send_async(AddProgress::Abort(e.into())).await.ok();
            }
        });
    }

    receiver.into_stream().map(ImportBlobEvent)
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ImportBlobEvent(pub AddProgress);

async fn add_from_path<S: Store>(
    store: S,
    path: PathBuf,
    progress: flume::Sender<AddProgress>,
) -> Result<()> {
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
    let (tag, _size) = store
        .import_file(path, import_mode, BlobFormat::Raw, import_progress)
        .await?;

    let hash_and_format = tag.inner();
    let HashAndFormat { hash, format } = *hash_and_format;
    let tag = store.create_tag(*hash_and_format).await?;
    progress
        .send(AddProgress::AllDone { hash, format, tag })
        .await?;
    Ok(())
}
