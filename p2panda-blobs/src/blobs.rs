// SPDX-License-Identifier: AGPL-3.0-or-later

use std::path::PathBuf;

use anyhow::Result;
use futures_util::Stream;
use iroh_blobs::downloader::Downloader;
use iroh_blobs::store::Store;
use p2panda_core::Hash;
use p2panda_net::{Network, NetworkBuilder};
use tokio_util::task::LocalPoolHandle;

use crate::download::download_blob;
use crate::import::{import_blob, ImportBlobEvent};
use crate::protocol::{BlobsProtocol, BLOBS_ALPN};
use crate::DownloadBlobEvent;

pub struct Blobs<S>
where
    S: Store,
{
    downloader: Downloader,
    network: Network,
    pool_handle: LocalPoolHandle,
    store: S,
}

impl<S> Blobs<S>
where
    S: Store,
{
    pub async fn from_builder(
        network_builder: NetworkBuilder,
        store: S,
    ) -> Result<(Network, Self)> {
        let pool_handle = LocalPoolHandle::new(num_cpus::get());

        let network = network_builder
            .protocol(
                BLOBS_ALPN,
                BlobsProtocol::new(store.clone(), pool_handle.clone()),
            )
            .build()
            .await?;

        let downloader = Downloader::new(
            store.clone(),
            network.endpoint().clone(),
            pool_handle.clone(),
        );

        let blobs = Self {
            downloader,
            network: network.clone(),
            pool_handle,
            store,
        };

        Ok((network, blobs))
    }

    pub async fn import_blob(&self, path: PathBuf) -> impl Stream<Item = ImportBlobEvent> {
        import_blob(self.store.clone(), self.pool_handle.clone(), path).await
    }

    pub async fn download_blob(&self, hash: Hash) -> impl Stream<Item = DownloadBlobEvent> {
        download_blob(
            self.network.endpoint().clone(),
            self.downloader.clone(),
            self.pool_handle.clone(),
            // @TODO: Get currently known peers from network and use them here
            vec![],
            hash,
        )
        .await
    }
}
