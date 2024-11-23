use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::io;
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;
use futures_lite::Stream;
use iroh_blobs::store::bao_tree::io::{fsm::Outboard, outboard::PreOrderOutboard};
use iroh_blobs::store::bao_tree::BaoTree;
use iroh_blobs::store::{
    BaoBatchWriter, BaoBlobSize, ConsistencyCheckProgress, ExportProgressCb, GcConfig, ImportMode,
    ImportProgress, Map, MapEntry, MapEntryMut, MapMut, ReadableStore, Store,
};
use iroh_blobs::util::progress::{BoxedProgressSender, IdGenerator, ProgressSender};
use iroh_blobs::util::SparseMemFile;
use iroh_blobs::{BlobFormat, HashAndFormat, Tag, TempTag, IROH_BLOCK_SIZE};
use iroh_io::AsyncSliceReader;
use s3::Bucket;
use tokio::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use tracing::{trace, warn};

use crate::bao_file::{BaoFileHandle, BaoMeta};
use crate::paths::{Paths, META_SUFFIX, NO_PREFIX};
use crate::utils::{get_meta, get_outboard, put_meta};
use crate::{
    BlobHash, CompletedBlob, IncompleteBlob, NotImportedObject, ObjectSize, SignedBlobInfo,
    UnsignedBlobInfo,
};

/// An S3 backed iroh blobs store.
///
/// Blob data and outboard files are stored in an S3 bucket.
#[derive(Debug, Clone)]
pub struct S3Store {
    buckets: Vec<Bucket>,
    inner: Arc<S3StoreInner>,
}

impl S3Store {
    /// Create a new S3 blob store interface for p2panda.
    pub async fn new(buckets: Vec<Bucket>) -> Result<Self> {
        let mut store = Self {
            buckets,
            inner: Default::default(),
        };
        store.init().await?;
        Ok(store)
    }

    /// Returns a list of all buckets managed by this store.
    pub fn buckets(&self) -> &Vec<Bucket> {
        self.buckets.as_ref()
    }

    /// Initiate the blob store from the contents of given S3 buckets.
    ///
    /// This method looks at all meta files present on the configured S3 buckets and establishes an
    /// index.
    async fn init(&mut self) -> Result<()> {
        for bucket in &self.buckets {
            let results = bucket.list(NO_PREFIX, None).await?;
            for list in results {
                for object in list.contents {
                    if object.key.ends_with(META_SUFFIX) {
                        let paths = Paths::from_meta(&object.key);
                        let meta = get_meta(bucket, &paths).await?;
                        let outboard = match get_outboard(bucket, &paths).await {
                            Ok(outboard) => outboard,
                            Err(_) => {
                                trace!("no outboard file found for blob {}", meta.hash);
                                SparseMemFile::new()
                            }
                        };
                        let bao_file =
                            BaoFileHandle::new(bucket.clone(), paths, outboard, meta.size);
                        let entry = Entry::new(bao_file, meta);
                        self.write_lock().await.entries.insert(entry.hash(), entry);
                    };
                }
            }
        }

        Ok(())
    }

    /// Import a new blob from S3 bucket.
    ///
    /// This method should be called as soon as a new S3 object was discovered in one of the
    /// buckets. We need to import it here to prepare the object for p2p sync.
    ///
    /// Several tasks are needed to do that:
    /// - Process all blob bytes and create an "outboard" bao4 file (this gives us the hash).
    /// - Create a "meta" file based on the provided path, size and calculated hash.
    /// - Upload both of these to the S3 bucket.
    /// - Insert an `Entry` into the index to represent this new blob>
    pub async fn import_object(&self, object: NotImportedObject) -> Result<()> {
        let bucket = self.bucket(&object.local_bucket_name);
        let (bao_file, meta) =
            BaoFileHandle::from_local_object(bucket, object.key, object.size).await?;
        let entry = Entry::new(bao_file, meta);
        self.write_lock().await.entries.insert(entry.hash(), entry);
        Ok(())
    }

    /// Tell the store about a new blob we discovered on the network and would like to download (at
    /// some point).
    ///
    /// Our store implementation expects that it knows about a blobs' path, size and signature
    /// _before_ the download of the blob actually occurs. This method is for informing the store
    /// of this meta data, however it doesn't trigger the download of the blob.
    ///
    /// No checks take place on the integrity of the signature, we assume that this has been
    /// handled before.
    pub async fn blob_discovered(&mut self, blob: SignedBlobInfo) -> Result<Option<Entry>> {
        // If we already "discovered" this blob then we don't need to do anything.
        if self.read_lock().await.entries.contains_key(&blob.hash) {
            return Ok(None)
        };

        let bucket = self.bucket(&blob.local_bucket_name);
        let paths = Paths::new(&blob.key);
        let meta = BaoMeta {
            hash: blob.hash,
            key: blob.key,
            size: blob.size,
            complete: false,
            remote_bucket_name: blob.remote_bucket_name,
            public_key: Some(blob.public_key),
            signature: Some(blob.signature),
        };
        put_meta(&bucket, &paths, &meta).await?;
        let bao_file = BaoFileHandle::new(bucket, paths, SparseMemFile::new(), blob.size);
        let entry = Entry::new(bao_file, meta);
        Ok(self.write_lock().await.entries.insert(blob.hash, entry))
    }

    /// Query the store for all complete blobs.
    pub async fn complete_blobs(&self) -> Vec<CompletedBlob> {
        let entries = self.read_lock().await.entries.clone();
        entries
            .into_values()
            .filter(|entry| entry.meta.complete)
            .map(|entry| match entry.meta.public_key {
                Some(public_key) => CompletedBlob::Signed(SignedBlobInfo {
                    hash: entry.hash(),
                    remote_bucket_name: entry.meta.remote_bucket_name,
                    local_bucket_name: entry.bucket_name,
                    key: entry.meta.key,
                    size: entry.meta.size,
                    public_key,
                    signature: entry
                        .meta
                        .signature
                        .expect("signature needs to exist next to public key"),
                }),
                None => CompletedBlob::Unsigned(UnsignedBlobInfo {
                    hash: entry.hash(),
                    local_bucket_name: entry.bucket_name,
                    key: entry.meta.key,
                    size: entry.meta.size,
                }),
            })
            .collect()
    }

    /// Query the store for all incomplete blobs.
    pub async fn incomplete_blobs(&self) -> Vec<IncompleteBlob> {
        let entries = self.read_lock().await.entries.clone();
        entries
            .into_values()
            .filter(|x| !x.meta.complete)
            .map(|entry| IncompleteBlob {
                hash: entry.hash(),
                local_bucket_name: entry.bucket_name,
                remote_bucket_name: entry.meta.remote_bucket_name,
                key: entry.meta.key,
                size: entry.meta.size,
                public_key: entry
                    .meta
                    .public_key
                    .expect("incomplete blobs from remote peers need to be signed"),
                signature: entry
                    .meta
                    .signature
                    .expect("incomplete blobs from remote peers need to be signed"),
            })
            .collect()
    }

    async fn write_lock(&self) -> RwLockWriteGuard<'_, StateInner> {
        self.inner.0.write().await
    }

    async fn read_lock(&self) -> RwLockReadGuard<'_, StateInner> {
        self.inner.0.read().await
    }

    fn bucket(&self, bucket_name: &str) -> Bucket {
        self.buckets
            .iter()
            .find(|bucket| bucket.name() == bucket_name)
            .expect("bucket should exist")
            .clone()
    }
}

impl Store for S3Store {
    async fn import_file(
        &self,
        _path: std::path::PathBuf,
        _mode: ImportMode,
        _format: BlobFormat,
        _progress: impl ProgressSender<Msg = ImportProgress> + IdGenerator,
    ) -> io::Result<(TempTag, u64)> {
        unimplemented!()
    }

    async fn import_stream(
        &self,
        _data: impl Stream<Item = io::Result<Bytes>> + Unpin + Send + 'static,
        _format: BlobFormat,
        _progress: impl ProgressSender<Msg = ImportProgress> + IdGenerator,
    ) -> io::Result<(TempTag, u64)> {
        unimplemented!()
    }

    async fn import_bytes(&self, _bytes: Bytes, _format: BlobFormat) -> io::Result<TempTag> {
        unimplemented!()
    }

    async fn set_tag(&self, _name: Tag, _value: Option<HashAndFormat>) -> io::Result<()> {
        unimplemented!()
    }

    async fn create_tag(&self, _hash: HashAndFormat) -> io::Result<Tag> {
        unimplemented!()
    }

    fn temp_tag(&self, _tag: HashAndFormat) -> TempTag {
        unimplemented!()
    }

    async fn gc_run<G, Gut>(&self, _config: GcConfig, _protected_cb: G)
    where
        G: Fn() -> Gut,
        Gut: Future<Output = BTreeSet<BlobHash>> + Send,
    {
        unimplemented!()
    }

    async fn delete(&self, _hashes: Vec<BlobHash>) -> io::Result<()> {
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
    entries: BTreeMap<BlobHash, Entry>,
}

/// An in-memory entry.
#[derive(Debug, Clone)]
pub struct Entry {
    pub bucket_name: String,
    pub meta: BaoMeta,
    pub paths: Paths,
    inner: Arc<EntryInner>,
}

impl Entry {
    pub fn new(bao_file: BaoFileHandle, meta: BaoMeta) -> Self {
        let paths = Paths::new(&meta.key);
        let bucket_name = bao_file.data.bucket_name();

        Entry {
            inner: Arc::new(EntryInner {
                hash: meta.hash,
                size: meta.size,
                data: RwLock::new(bao_file),
            }),
            bucket_name,
            meta,
            paths,
        }
    }
}

#[derive(Debug)]
struct EntryInner {
    hash: BlobHash,
    size: ObjectSize,
    data: RwLock<BaoFileHandle>,
}

impl MapEntry for Entry {
    fn hash(&self) -> BlobHash {
        self.inner.hash
    }

    fn size(&self) -> BaoBlobSize {
        let size = self.inner.size;
        BaoBlobSize::new(size, self.meta.complete)
    }

    fn is_complete(&self) -> bool {
        self.meta.complete
    }

    async fn outboard(&self) -> io::Result<impl Outboard> {
        let size = self.inner.data.read().await.data_len();
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
        self.0.data.read().await.read_data_at(offset, len).await
    }

    async fn size(&mut self) -> std::io::Result<u64> {
        let size = self.0.data.read().await.data_len();
        Ok(size)
    }
}

struct OutboardReader(Arc<EntryInner>);

impl AsyncSliceReader for OutboardReader {
    async fn read_at(&mut self, offset: u64, len: usize) -> std::io::Result<Bytes> {
        self.0.data.read().await.read_outboard_at(offset, len).await
    }

    async fn size(&mut self) -> std::io::Result<u64> {
        self.0
            .data
            .read()
            .await
            .outboard_len()
            .await
            .map_err(io::Error::other)
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
        let result = self.0.data.write().await.write_batch(size, &batch).await;
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
        let mut data = self.0.data.write().await;
        data.complete().await.map_err(io::Error::other)
    }
}

impl Map for S3Store {
    type Entry = Entry;

    async fn get(&self, hash: &BlobHash) -> std::io::Result<Option<Self::Entry>> {
        Ok(self.read_lock().await.entries.get(hash).cloned())
    }
}

impl MapMut for S3Store {
    type EntryMut = Entry;

    async fn get_mut(&self, hash: &BlobHash) -> std::io::Result<Option<Self::EntryMut>> {
        self.get(hash).await
    }

    async fn get_or_create(&self, hash: BlobHash, _size: ObjectSize) -> std::io::Result<Entry> {
        if let Some(entry) = self.get(&hash).await? {
            return Ok(entry);
        }

        // We expect all entries to already have been added to the store before this method is
        // called during download of a new blob from the network.
        unimplemented!()
    }

    async fn entry_status(
        &self,
        hash: &BlobHash,
    ) -> std::io::Result<iroh_blobs::store::EntryStatus> {
        self.entry_status_sync(hash)
    }

    fn entry_status_sync(
        &self,
        _hash: &BlobHash,
    ) -> std::io::Result<iroh_blobs::store::EntryStatus> {
        unimplemented!()
    }

    async fn insert_complete(&self, mut entry: Entry) -> std::io::Result<()> {
        entry.meta.complete = true;
        let paths = Paths::new(&entry.meta.key);
        let bucket = self.bucket(&entry.bucket_name);
        put_meta(&bucket, &paths, &entry.meta)
            .await
            .map_err(io::Error::other)?;

        let hash = entry.hash();
        let mut inner = self.write_lock().await;
        inner.entries.insert(hash, entry);

        Ok(())
    }
}

impl ReadableStore for S3Store {
    async fn blobs(&self) -> io::Result<iroh_blobs::store::DbIter<BlobHash>> {
        let entries = self.read_lock().await.entries.clone();
        Ok(Box::new(
            entries
                .into_values()
                .filter(|x| x.meta.complete)
                .map(|x| Ok(x.hash())),
        ))
    }

    async fn partial_blobs(&self) -> io::Result<iroh_blobs::store::DbIter<BlobHash>> {
        let entries = self.read_lock().await.entries.clone();
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
        _hash: BlobHash,
        _target: std::path::PathBuf,
        _mode: iroh_blobs::store::ExportMode,
        _progress: ExportProgressCb,
    ) -> io::Result<()> {
        unimplemented!()
    }
}
