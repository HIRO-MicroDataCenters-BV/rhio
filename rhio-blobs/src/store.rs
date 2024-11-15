use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::io;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

use bytes::Bytes;
use futures_lite::Stream;
use iroh_blobs::store::bao_tree::io::{fsm::Outboard, outboard::PreOrderOutboard};
use iroh_blobs::store::bao_tree::BaoTree;
use iroh_blobs::store::{
    BaoBatchWriter, ConsistencyCheckProgress, ExportProgressCb, ImportMode, ImportProgress, Map,
};
use iroh_blobs::store::{BaoBlobSize, MapEntry, MapEntryMut, ReadableStore};
use iroh_blobs::store::{GcConfig, MapMut};
use iroh_blobs::util::progress::{BoxedProgressSender, IdGenerator, ProgressSender};
use iroh_blobs::{store::Store, BlobFormat, Hash, HashAndFormat};
use iroh_blobs::{Tag, TempTag, IROH_BLOCK_SIZE};
use iroh_io::AsyncSliceReader;
use s3::Bucket;
use tracing::warn;

use crate::bao_file::{get_meta, put_meta, BaoFileHandle, BaoMeta};
use crate::paths::Paths;

/// An s3 backed iroh blobs store.
///
/// Blob data and outboard files are stored in an s3 bucket.
#[derive(Debug, Clone)]
pub struct S3Store {
    bucket: Bucket,
    inner: Arc<S3StoreInner>,
}

impl S3Store {
    /// Create a new in memory store
    pub async fn new(bucket: Bucket) -> anyhow::Result<Self> {
        let mut store = Self {
            bucket,
            inner: Default::default(),
        };
        store.init().await?;
        Ok(store)
    }

    async fn init(&mut self) -> anyhow::Result<()> {
        let results = self
            .bucket
            .list("/".to_string(), None)
            .await
            .map_err(io::Error::other)?;

        for list in results {
            for object in list.contents {
                if object.key.ends_with(".meta") {
                    let meta = get_meta(&self.bucket, object.key).await?;
                    let paths = Paths::new(meta.path.clone());
                    let bao_file = BaoFileHandle::new(self.bucket.clone(), paths, meta.size);
                    let entry = Entry::new(bao_file, meta);
                    self.write_lock().entries.insert(entry.hash(), entry);
                };
            }
        }

        Ok(())
    }

    /// Import and complete existing object from an s3 bucket.
    ///
    /// This method processes the object bytes, generates a BAO outboard file, uploads this back
    /// to the s3 bucket adding the `.bao4` suffix, and then inserts the resulting Entry into an
    /// in-memory store.
    pub async fn import_object(&self, path: String, size: u64) -> anyhow::Result<()> {
        // Create a new BAO file from existing data, this processes all bytes and uploads an
        // outboard file to the s3 bucket.
        let (bao_file, meta) =
            BaoFileHandle::create_complete(self.bucket.clone(), path, size).await?;
        let entry = Entry::new(bao_file, meta);
        self.write_lock().entries.insert(entry.hash(), entry);
        Ok(())
    }

    pub async fn blob_discovered(
        &mut self,
        hash: Hash,
        path: String,
        size: u64,
    ) -> anyhow::Result<Option<Entry>> {
        let paths = Paths::new(path.clone());
        let meta = BaoMeta {
            hash,
            path,
            size,
            complete: false,
        };
        put_meta(&self.bucket, &paths, &meta).await?;
        let bao_file = BaoFileHandle::new(self.bucket.clone(), paths, size);
        let entry: Entry = Entry::new(bao_file, meta);
        Ok(self.write_lock().entries.insert(hash, entry))
    }

    pub fn complete_blobs(&self) -> io::Result<iroh_blobs::store::DbIter<BaoMeta>> {
        let entries = self.read_lock().entries.clone();
        Ok(Box::new(
            entries
                .into_values()
                .filter(|x| x.meta.complete)
                .map(|x| Ok(x.meta)),
        ))
    }

    pub fn incomplete_blobs(&self) -> io::Result<iroh_blobs::store::DbIter<BaoMeta>> {
        let entries = self.read_lock().entries.clone();
        Ok(Box::new(
            entries
                .into_values()
                .filter(|x| !x.meta.complete)
                .map(|x| Ok(x.meta)),
        ))
    }

    /// Take a write lock on the store
    fn write_lock(&self) -> RwLockWriteGuard<'_, StateInner> {
        self.inner.0.write().unwrap()
    }

    /// Take a read lock on the store
    fn read_lock(&self) -> RwLockReadGuard<'_, StateInner> {
        self.inner.0.read().unwrap()
    }
}

impl Store for S3Store {
    async fn import_file(
        &self,
        path: std::path::PathBuf,
        _mode: ImportMode,
        format: BlobFormat,
        progress: impl ProgressSender<Msg = ImportProgress> + IdGenerator,
    ) -> io::Result<(TempTag, u64)> {
        unimplemented!()
    }

    async fn import_stream(
        &self,
        mut data: impl Stream<Item = io::Result<Bytes>> + Unpin + Send + 'static,
        format: BlobFormat,
        progress: impl ProgressSender<Msg = ImportProgress> + IdGenerator,
    ) -> io::Result<(TempTag, u64)> {
        unimplemented!()
    }

    async fn import_bytes(&self, bytes: Bytes, format: BlobFormat) -> io::Result<TempTag> {
        unimplemented!()
    }

    async fn set_tag(&self, name: Tag, value: Option<HashAndFormat>) -> io::Result<()> {
        unimplemented!()
    }

    async fn create_tag(&self, hash: HashAndFormat) -> io::Result<Tag> {
        unimplemented!()
    }

    fn temp_tag(&self, tag: HashAndFormat) -> TempTag {
        unimplemented!()
    }

    async fn gc_run<G, Gut>(&self, config: GcConfig, protected_cb: G)
    where
        G: Fn() -> Gut,
        Gut: Future<Output = BTreeSet<Hash>> + Send,
    {
        unimplemented!()
    }

    async fn delete(&self, hashes: Vec<Hash>) -> io::Result<()> {
        unimplemented!()
    }

    async fn shutdown(&self) {}

    async fn sync(&self) -> io::Result<()> {
        Ok(())
    }
}

#[derive(Debug, Default)]
struct S3StoreInner(RwLock<StateInner>);

#[derive(Debug, Default)]
struct StateInner {
    entries: BTreeMap<Hash, Entry>,
}

/// An in memory entry
#[derive(Debug, Clone)]
pub struct Entry {
    meta: BaoMeta,
    inner: Arc<EntryInner>,
}

impl Entry {
    pub fn new(bao_file: BaoFileHandle, meta: BaoMeta) -> Self {
        Entry {
            inner: Arc::new(EntryInner {
                hash: meta.hash,
                data: RwLock::new(bao_file),
            }),
            meta,
        }
    }
}

#[derive(Debug)]
struct EntryInner {
    hash: Hash,
    data: RwLock<BaoFileHandle>,
}

impl MapEntry for Entry {
    fn hash(&self) -> Hash {
        self.inner.hash
    }

    fn size(&self) -> BaoBlobSize {
        let size = self.inner.data.read().unwrap().data_len();
        BaoBlobSize::new(size, self.meta.complete)
    }

    fn is_complete(&self) -> bool {
        self.meta.complete
    }

    async fn outboard(&self) -> io::Result<impl Outboard> {
        let size = self.inner.data.read().unwrap().data_len();
        Ok(PreOrderOutboard {
            root: self.hash().into(),
            tree: BaoTree::new(size, IROH_BLOCK_SIZE),
            data: OutboardReader(self.inner.clone()),
        })
    }

    async fn data_reader(&self) -> io::Result<impl AsyncSliceReader> {
        Ok(DataReader(self.inner.clone()))
    }
}

impl MapEntryMut for Entry {
    async fn batch_writer(&self) -> io::Result<impl BaoBatchWriter> {
        Ok(BatchWriter(self.inner.clone()))
    }
}

struct DataReader(Arc<EntryInner>);

impl AsyncSliceReader for DataReader {
    async fn read_at(&mut self, offset: u64, len: usize) -> std::io::Result<Bytes> {
        self.0.data.read().unwrap().read_data_at(offset, len).await
    }

    async fn size(&mut self) -> std::io::Result<u64> {
        let size = self.0.data.read().unwrap().data_len();
        Ok(size)
    }
}

struct OutboardReader(Arc<EntryInner>);

impl AsyncSliceReader for OutboardReader {
    async fn read_at(&mut self, offset: u64, len: usize) -> std::io::Result<Bytes> {
        self.0
            .data
            .read()
            .unwrap()
            .read_outboard_at(offset, len)
            .await
    }

    async fn size(&mut self) -> std::io::Result<u64> {
        self.0
            .data
            .read()
            .unwrap()
            .outboard_len()
            .await
            .map_err(|err| io::Error::other(err))
    }
}

struct BatchWriter(Arc<EntryInner>);

impl BaoBatchWriter for BatchWriter {
    /// Write a batch of items to the s3 hosted bao file.
    async fn write_batch(
        &mut self,
        size: u64,
        batch: Vec<iroh_blobs::store::bao_tree::io::fsm::BaoContentItem>,
    ) -> io::Result<()> {
        let result = self.0.data.write().unwrap().write_batch(size, &batch).await;
        if let Err(err) = result {
            warn!("Error writing BAO content: {err}");
            Err(io::Error::other(err))
        } else {
            Ok(())
        }
    }

    /// Sync any remaining bytes to the remote store.
    ///
    /// MUST be called once all bytes have been processed via `write_batch` in order to complete the
    /// underlying multipart upload.
    async fn sync(&mut self) -> io::Result<()> {
        self.0
            .data
            .write()
            .unwrap()
            .complete()
            .await
            .map_err(io::Error::other)
    }
}

impl Map for S3Store {
    type Entry = Entry;

    async fn get(&self, hash: &Hash) -> std::io::Result<Option<Self::Entry>> {
        Ok(self.inner.0.read().unwrap().entries.get(hash).cloned())
    }
}

impl MapMut for S3Store {
    type EntryMut = Entry;

    async fn get_mut(&self, hash: &Hash) -> std::io::Result<Option<Self::EntryMut>> {
        self.get(hash).await
    }

    async fn get_or_create(&self, hash: Hash, _size: u64) -> std::io::Result<Entry> {
        if let Some(entry) = self.get(&hash).await? {
            return Ok(entry);
        }

        // We expect all entries to already have been added to the store before this method is
        // called during download of a new blob from the network.
        unimplemented!()
    }

    async fn entry_status(&self, hash: &Hash) -> std::io::Result<iroh_blobs::store::EntryStatus> {
        self.entry_status_sync(hash)
    }

    fn entry_status_sync(&self, hash: &Hash) -> std::io::Result<iroh_blobs::store::EntryStatus> {
        Ok(match self.inner.0.read().unwrap().entries.get(hash) {
            Some(entry) => {
                if entry.meta.complete {
                    iroh_blobs::store::EntryStatus::Complete
                } else {
                    iroh_blobs::store::EntryStatus::Partial
                }
            }
            None => iroh_blobs::store::EntryStatus::NotFound,
        })
    }

    async fn insert_complete(&self, mut entry: Entry) -> std::io::Result<()> {
        entry.meta.complete = true;
        let paths = Paths::new(entry.meta.path.clone());
        put_meta(&self.bucket, &paths, &entry.meta)
            .await
            .map_err(io::Error::other)?;

        let hash = entry.hash();
        let mut inner = self.inner.0.write().unwrap();
        inner.entries.insert(hash, entry);

        Ok(())
    }
}

impl ReadableStore for S3Store {
    async fn blobs(&self) -> io::Result<iroh_blobs::store::DbIter<Hash>> {
        let entries = self.read_lock().entries.clone();
        Ok(Box::new(
            entries
                .into_values()
                .filter(|x| x.meta.complete)
                .map(|x| Ok(x.hash())),
        ))
    }

    async fn partial_blobs(&self) -> io::Result<iroh_blobs::store::DbIter<Hash>> {
        let entries = self.read_lock().entries.clone();
        Ok(Box::new(
            entries
                .into_values()
                .filter(|x| !x.meta.complete)
                .map(|x| Ok(x.hash())),
        ))
    }

    async fn tags(&self) -> io::Result<iroh_blobs::store::DbIter<(Tag, HashAndFormat)>> {
        unimplemented!()
    }

    fn temp_tags(&self) -> Box<dyn Iterator<Item = HashAndFormat> + Send + Sync + 'static> {
        unimplemented!()
    }

    async fn consistency_check(
        &self,
        _repair: bool,
        _tx: BoxedProgressSender<ConsistencyCheckProgress>,
    ) -> io::Result<()> {
        unimplemented!()
    }

    async fn export(
        &self,
        hash: Hash,
        target: std::path::PathBuf,
        mode: iroh_blobs::store::ExportMode,
        progress: ExportProgressCb,
    ) -> io::Result<()> {
        unimplemented!()
    }
}

#[derive(Debug, Default, Clone)]
struct TempCounters {
    /// number of raw temp tags for a hash
    raw: u64,
    /// number of hash seq temp tags for a hash
    hash_seq: u64,
}

impl TempCounters {
    fn counter(&mut self, format: BlobFormat) -> &mut u64 {
        match format {
            BlobFormat::Raw => &mut self.raw,
            BlobFormat::HashSeq => &mut self.hash_seq,
        }
    }

    fn inc(&mut self, format: BlobFormat) {
        let counter = self.counter(format);
        *counter = counter.checked_add(1).unwrap();
    }

    fn dec(&mut self, format: BlobFormat) {
        let counter = self.counter(format);
        *counter = counter.saturating_sub(1);
    }

    fn is_empty(&self) -> bool {
        self.raw == 0 && self.hash_seq == 0
    }
}

#[derive(Debug, Clone, Default)]
struct TempCounterMap(std::collections::BTreeMap<Hash, TempCounters>);

impl TempCounterMap {
    fn inc(&mut self, value: &HashAndFormat) {
        let HashAndFormat { hash, format } = value;
        self.0.entry(*hash).or_default().inc(*format)
    }

    fn dec(&mut self, value: &HashAndFormat) {
        let HashAndFormat { hash, format } = value;
        let Some(counters) = self.0.get_mut(hash) else {
            warn!("Decrementing non-existent temp tag");
            return;
        };
        counters.dec(*format);
        if counters.is_empty() {
            self.0.remove(hash);
        }
    }

    fn contains(&self, hash: &Hash) -> bool {
        self.0.contains_key(hash)
    }

    fn keys(&self) -> impl Iterator<Item = HashAndFormat> {
        let mut res = Vec::new();
        for (k, v) in self.0.iter() {
            if v.raw > 0 {
                res.push(HashAndFormat::raw(*k));
            }
            if v.hash_seq > 0 {
                res.push(HashAndFormat::hash_seq(*k));
            }
        }
        res.into_iter()
    }
}
