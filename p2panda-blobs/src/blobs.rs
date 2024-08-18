// SPDX-License-Identifier: AGPL-3.0-or-later

use std::io;
use std::path::PathBuf;

use anyhow::Result;
use bytes::Bytes;
use futures_util::Stream;
use iroh_base::hash::Hash as IrohHash;
use iroh_blobs::downloader::Downloader;
use iroh_blobs::store::{Map, Store};
use p2panda_core::Hash;
use p2panda_net::{Network, NetworkBuilder};
use tokio_util::task::LocalPoolHandle;

use crate::download::download_blob;
use crate::export::export_blob;
use crate::import::{import_blob, import_blob_from_stream, ImportBlobEvent};
use crate::protocol::{BlobsProtocol, BLOBS_ALPN};
use crate::DownloadBlobEvent;

#[derive(Clone, Debug)]
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
    S: Store + Clone + Send + Sync + 'static,
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

    pub async fn get(&self, hash: Hash) -> anyhow::Result<Option<<S as Map>::Entry>> {
        let hash = IrohHash::from_bytes(*hash.as_bytes());
        let entry = self.store.get(&hash).await?;
        Ok(entry)
    }

    pub async fn import_blob(&self, path: PathBuf) -> impl Stream<Item = ImportBlobEvent> {
        import_blob(self.store.clone(), self.pool_handle.clone(), path).await
    }

    pub async fn import_blob_from_stream<T>(&self, data: T) -> impl Stream<Item = ImportBlobEvent>
    where
        T: Stream<Item = io::Result<Bytes>> + Send + Unpin + 'static,
    {
        import_blob_from_stream(self.store.clone(), self.pool_handle.clone(), data).await
    }

    pub async fn download_blob(&self, hash: Hash) -> impl Stream<Item = DownloadBlobEvent> {
        download_blob(
            self.network.clone(),
            self.downloader.clone(),
            self.pool_handle.clone(),
            hash,
        )
        .await
    }

    pub async fn export_blob(&self, hash: Hash, path: &PathBuf) -> Result<()> {
        export_blob(&self.store, hash, path).await?;
        Ok(())
    }
}
