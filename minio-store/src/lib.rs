use std::io;

use bytes::Bytes;
use futures::Stream;
use iroh_blobs::store::mem::Entry;
use iroh_blobs::store::{
    ConsistencyCheckProgress, DbIter, ExportMode, ExportProgressCb, ImportMode, ImportProgress,
    Map, MapMut, ReadableStore, Store,
};
use iroh_blobs::util::progress::{BoxedProgressSender, IdGenerator, ProgressSender};
use iroh_blobs::{BlobFormat, Hash, HashAndFormat, Tag, TempTag};

#[derive(Clone, Debug)]
struct MinioStore {}

impl MapMut for MinioStore {
    type EntryMut = Entry;

    async fn get_mut(&self, hash: &Hash) -> io::Result<Option<Self::EntryMut>> {
        todo!()
    }

    async fn get_or_create(&self, hash: Hash, size: u64) -> io::Result<Self::EntryMut> {
        todo!()
    }

    async fn entry_status(&self, hash: &Hash) -> io::Result<iroh_blobs::store::EntryStatus> {
        todo!()
    }

    fn entry_status_sync(&self, hash: &Hash) -> io::Result<iroh_blobs::store::EntryStatus> {
        todo!()
    }

    async fn insert_complete(&self, entry: Self::EntryMut) -> io::Result<()> {
        todo!()
    }
}

impl Map for MinioStore {
    // @TODO: this uses the memory store from iroh_blobs::store::mem but we will probably
    // need to implement our own.
    type Entry = Entry;

    async fn get(&self, hash: &Hash) -> io::Result<Option<Self::Entry>> {
        todo!()
    }
}

impl ReadableStore for MinioStore {
    async fn blobs(&self) -> io::Result<DbIter<Hash>> {
        todo!()
    }

    async fn tags(&self) -> io::Result<DbIter<(Tag, HashAndFormat)>> {
        todo!()
    }

    fn temp_tags(&self) -> Box<dyn Iterator<Item = HashAndFormat> + Send + Sync + 'static> {
        todo!()
    }

    async fn consistency_check(
        &self,
        repair: bool,
        tx: BoxedProgressSender<ConsistencyCheckProgress>,
    ) -> io::Result<()> {
        todo!()
    }

    async fn partial_blobs(&self) -> io::Result<DbIter<Hash>> {
        todo!()
    }

    async fn export(
        &self,
        hash: Hash,
        target: std::path::PathBuf,
        mode: ExportMode,
        progress: ExportProgressCb,
    ) -> io::Result<()> {
        todo!()
    }
}

impl Store for MinioStore {
    async fn import_file(
        &self,
        data: std::path::PathBuf,
        mode: ImportMode,
        format: BlobFormat,
        progress: impl ProgressSender<Msg = ImportProgress> + IdGenerator,
    ) -> io::Result<(iroh_blobs::TempTag, u64)> {
        todo!()
    }

    async fn import_bytes(
        &self,
        bytes: Bytes,
        format: BlobFormat,
    ) -> io::Result<iroh_blobs::TempTag> {
        todo!()
    }

    async fn import_stream(
        &self,
        mut data: impl Stream<Item = io::Result<Bytes>> + Unpin + Send + 'static,
        format: BlobFormat,
        progress: impl ProgressSender<Msg = ImportProgress> + IdGenerator,
    ) -> io::Result<(iroh_blobs::TempTag, u64)> {
        todo!()
    }

    async fn set_tag(&self, name: Tag, hash: Option<HashAndFormat>) -> io::Result<()> {
        todo!()
    }

    async fn create_tag(&self, hash: HashAndFormat) -> io::Result<Tag> {
        todo!()
    }

    fn temp_tag(&self, value: HashAndFormat) -> TempTag {
        todo!()
    }

    async fn gc_start(&self) -> io::Result<()> {
        todo!()
    }

    async fn delete(&self, hashes: Vec<Hash>) -> io::Result<()> {
        todo!()
    }

    async fn shutdown(&self) {
        todo!()
    }
}
