//! An implementation of a bao file, meaning some data blob with associated
//! outboard.
//!
//! The actual blob data is stored on an s3 bucket and the outboard files are stored on the local
//! filesystem. A strict requirement of this implementation is that the amount of data being
//! processed in memory is bounded to a reasonably low limit. To achieve this data is uploaded to
//! the configured s3 bucket as the BAO tree is being constructed.
//!
//! For this implementation it is assumed that the final size of a blob is known _before_ adding
//! it to the store.
use std::{
    fs::{File, OpenOptions},
    io,
    ops::{Deref, DerefMut},
    path::{Path, PathBuf},
    sync::{Arc, RwLock, Weak},
};

use anyhow::Result;
use bytes::{Bytes, BytesMut};
use derive_more::Debug;
use iroh_base::hash::Hash;
use iroh_blobs::{
    store::{
        bao_tree::{
            io::{
                fsm::BaoContentItem,
                outboard::PreOrderOutboard,
                sync::{ReadAt, WriteAt},
            },
            BaoTree,
        },
        BaoBatchWriter,
    },
    IROH_BLOCK_SIZE,
};
use iroh_io::{AsyncSliceReader, HttpAdapter};
use s3::Bucket;

use super::s3_file::S3File;

/// Data paths for outboard and sizes files which are stored on the local filesystem.
pub struct DataPaths {
    /// The outboard file. This is *without* the size header, since that is not
    /// known for partial files.
    ///
    /// The size of the outboard file is therefore a multiple of a hash pair
    /// (64 bytes).
    ///
    /// The naming convention is to use obao for pre order traversal and oboa
    /// for post order traversal. The log2 of the chunk group size is appended,
    /// so for the default chunk group size in iroh of 4, the file extension
    /// is .obao4.
    pub outboard: PathBuf,
    /// The sizes file. This is a file with 8 byte sizes for each chunk group.
    /// The naming convention is to prepend the log2 of the chunk group size,
    /// so for the default chunk group size in iroh of 4, the file extension
    /// is .sizes4.
    ///
    /// The traversal order is not relevant for the sizes file, since it is
    /// about the data chunks, not the hash pairs.
    pub sizes: PathBuf,
}

/// Storage for complete blobs.
///
/// The data is stored on an s3 bucket at an arbitrary "paths" and the outboard files are stored on
/// the local filesystem at expected paths (based on hash string).
#[derive(derive_more::Debug)]
pub struct CompleteStorage {
    /// data part, which can be in memory or on disk.
    pub data: (S3File, u64),
    /// outboard part, which can be in memory or on disk.
    pub outboard: (File, u64),
}

impl CompleteStorage {
    /// Read from the data file at the given offset, until end of file or max bytes.
    pub async fn read_data_at(&self, offset: u64, len: usize) -> Result<Bytes> {
        let bytes = self.data.0.reader()?.read_at(offset, len).await?;
        Ok(bytes)
    }

    /// Read from the outboard file at the given offset, until end of file or max bytes.
    pub fn read_outboard_at(&self, offset: u64, len: usize) -> Bytes {
        read_to_end(&self.outboard.0, offset, len).unwrap()
    }

    /// The size of the data file.
    pub fn data_size(&self) -> u64 {
        self.data.1
    }

    /// The size of the outboard file.
    pub fn outboard_size(&self) -> u64 {
        self.outboard.1
    }
}

/// Create a file for reading and writing, but *without* truncating the existing
/// file.
fn create_read_write(path: impl AsRef<Path>) -> io::Result<File> {
    OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(path)
}

/// Create an `S3File` handle.
///
/// This doesn't make any calls to the remote bucket, it is just a configured handle from which
/// further requests can be made.
fn create_s3_read_write(bucket: Bucket, path: String, size: u64) -> S3File {
    S3File::new(bucket, path, size)
}

/// Read from the given file at the given offset, until end of file or max bytes.
fn read_to_end(file: impl ReadAt, offset: u64, max: usize) -> io::Result<Bytes> {
    let mut res = BytesMut::new();
    let mut buf = [0u8; 4096];
    let mut remaining = max;
    let mut offset = offset;
    while remaining > 0 {
        let end = buf.len().min(remaining);
        let read = file.read_at(offset, &mut buf[..end])?;
        if read == 0 {
            // eof
            break;
        }
        res.extend_from_slice(&buf[..read]);
        offset += read as u64;
        remaining -= read;
    }
    Ok(res.freeze())
}
// @TODO: Feels like we should still be using this, but I'm not sure where...
// fn max_offset(batch: &[BaoContentItem]) -> u64 {
//     batch
//         .iter()
//         .filter_map(|item| match item {
//             BaoContentItem::Leaf(leaf) => {
//                 let len = leaf.data.len().try_into().unwrap();
//                 let end = leaf
//                     .offset
//                     .checked_add(len)
//                     .expect("u64 overflow for leaf end");
//                 Some(end)
//             }
//             _ => None,
//         })
//         .max()
//         .unwrap_or(0)
// }

/// A file storage for an incomplete bao file.
#[derive(Debug)]
pub struct IncompleteStorage {
    pub data: S3File,
    pub outboard: std::fs::File,
    pub sizes: std::fs::File,
}

impl IncompleteStorage {
    /// Split into data, outboard and sizes files.
    pub fn into_parts(self) -> (S3File, File, File) {
        (self.data, self.outboard, self.sizes)
    }

    fn current_size(&self) -> io::Result<u64> {
        let len = self.sizes.metadata()?.len();
        if len < 8 {
            Ok(0)
        } else {
            // todo: use the last full u64 in case the sizes file is not a multiple of 8
            // bytes. Not sure how that would happen, but we should handle it.
            let mut buf = [0u8; 8];
            self.sizes.read_exact_at(len - 8, &mut buf)?;
            Ok(u64::from_le_bytes(buf))
        }
    }

    fn write_batch(&mut self, size: u64, batch: &[BaoContentItem]) -> Result<()> {
        let tree = BaoTree::new(size, IROH_BLOCK_SIZE);
        for item in batch {
            match item {
                BaoContentItem::Parent(parent) => {
                    if let Some(offset) = tree.pre_order_offset(parent.node) {
                        let o0 = offset * 64;
                        self.outboard
                            .write_all_at(o0, parent.pair.0.as_bytes().as_slice())?;
                        self.outboard
                            .write_all_at(o0 + 32, parent.pair.1.as_bytes().as_slice())?;
                    }
                }
                BaoContentItem::Leaf(leaf) => {
                    let o0 = leaf.offset;
                    // divide by chunk size, multiply by 8
                    let index = (leaf.offset >> (tree.block_size().chunk_log() + 10)) << 3;
                    tracing::trace!(
                        "write_batch f={:?} o={} l={}",
                        self.data,
                        o0,
                        leaf.data.len()
                    );

                    // @TODO: This will actually be a handle on some remote s3 data, we probably
                    // want to make this whole method async
                    self.data.write_all_at(o0, leaf.data.as_ref())?;

                    let size = tree.size();
                    self.sizes.write_all_at(index, &size.to_le_bytes())?;
                }
            }
        }
        Ok(())
    }

    async fn read_data_at(&self, offset: u64, len: usize) -> Result<Bytes> {
        let bytes = self.data.reader()?.read_at(offset, len).await?;
        Ok(bytes)
    }

    fn read_outboard_at(&self, offset: u64, len: usize) -> io::Result<Bytes> {
        read_to_end(&self.outboard, offset, len)
    }
}

/// The storage for a bao file split across local file system and an s3 bucket.
#[derive(Debug)]
pub(crate) enum BaoFileStorage {
    /// The entry is incomplete and on disk.
    IncompleteFile(IncompleteStorage),
    /// The entry is complete and on disk.
    ///
    /// Writing to this is a no-op, since it is already complete.
    Complete(CompleteStorage),
}

/// A weak reference to a bao file handle.
#[derive(Debug, Clone)]
pub struct BaoFileHandleWeak(Weak<BaoFileHandleInner>);

impl BaoFileHandleWeak {
    /// Upgrade to a strong reference if possible.
    pub fn upgrade(&self) -> Option<BaoFileHandle> {
        self.0.upgrade().map(BaoFileHandle)
    }

    /// True if the handle is still live (has strong references)
    pub fn is_live(&self) -> bool {
        self.0.strong_count() > 0
    }
}

/// The inner part of a bao file handle.
// @TODO: This needs to be converted into an s3 bao handle
#[derive(Debug)]
pub struct BaoFileHandleInner {
    pub(crate) storage: RwLock<BaoFileStorage>,
    config: Arc<BaoFileConfig>,
    hash: Hash,
}

/// A cheaply cloneable handle to a bao file, including the hash and the configuration.
#[derive(Debug, Clone, derive_more::Deref)]
pub struct BaoFileHandle(Arc<BaoFileHandleInner>);

/// Configuration for BAO file.
#[derive(derive_more::Debug, Clone)]
pub struct BaoFileConfig {
    /// Directory to store files in. Only used when memory limit is reached.
    root: Arc<PathBuf>,

    bucket: Bucket,
}

impl BaoFileConfig {
    /// Create a new deferred batch writer configuration.
    pub fn new(root: Arc<PathBuf>, bucket: Bucket) -> Self {
        Self { root, bucket }
    }

    /// Get the paths for a hash.
    pub fn paths(&self, hash: &Hash) -> DataPaths {
        DataPaths {
            outboard: self.root.join(format!("{}.obao4", hash.to_hex())),
            sizes: self.root.join(format!("{}.sizes4", hash.to_hex())),
        }
    }

    pub fn bucket(&self) -> Bucket {
        self.bucket.clone()
    }
}

/// A reader for the outboard part of a bao file.
#[derive(Debug)]
pub struct OutboardReader(BaoFileHandle);

impl AsyncSliceReader for OutboardReader {
    async fn read_at(&mut self, offset: u64, len: usize) -> io::Result<Bytes> {
        let lock = self.0.storage.read().unwrap();
        let bytes = match lock.deref() {
            BaoFileStorage::IncompleteFile(file_storage) => {
                file_storage.read_outboard_at(offset, len)
            }
            BaoFileStorage::Complete(complete_storage) => {
                Ok(complete_storage.read_outboard_at(offset, len))
            }
        }?;

        Ok(bytes)
    }

    async fn size(&mut self) -> io::Result<u64> {
        let lock = self.0.storage.read().unwrap();
        let size = match lock.deref() {
            BaoFileStorage::IncompleteFile(file_storage) => {
                file_storage.outboard.metadata().map(|m| m.len())
            }
            BaoFileStorage::Complete(complete_storage) => Ok(complete_storage.outboard_size()),
        }
        .map_err(|err| io::Error::other(err))?;

        Ok(size)
    }
}

impl BaoFileHandle {
    /// Create a new bao file handle with a partial file.
    pub fn incomplete_file(
        config: Arc<BaoFileConfig>,
        hash: Hash,
        path: String,
        size: u64,
    ) -> io::Result<Self> {
        let paths = config.paths(&hash);
        let storage = BaoFileStorage::IncompleteFile(IncompleteStorage {
            data: create_s3_read_write(config.bucket.clone(), path, size),
            outboard: create_read_write(&paths.outboard)?,
            sizes: create_read_write(&paths.sizes)?,
        });
        Ok(Self(Arc::new(BaoFileHandleInner {
            storage: RwLock::new(storage),
            config,
            hash,
        })))
    }

    /// Create a new complete bao file handle.
    pub fn new_complete(
        config: Arc<BaoFileConfig>,
        hash: Hash,
        data: (S3File, u64),
        outboard: (File, u64),
    ) -> Self {
        let storage = BaoFileStorage::Complete(CompleteStorage { data, outboard });
        Self(Arc::new(BaoFileHandleInner {
            storage: RwLock::new(storage),
            config,
            hash,
        }))
    }

    /// True if the file is complete.
    pub fn is_complete(&self) -> bool {
        matches!(
            self.storage.read().unwrap().deref(),
            BaoFileStorage::Complete(_)
        )
    }

    /// An AsyncSliceReader for the data file.
    ///
    /// Caution: this is a reader for the unvalidated data file. Reading this
    /// can produce data that does not match the hash.
    pub fn data_reader(&self) -> HttpAdapter {
        let lock = self.0.storage.read().unwrap();
        match lock.deref() {
            BaoFileStorage::IncompleteFile(file_storage) => file_storage.data.reader(),
            BaoFileStorage::Complete(complete_storage) => complete_storage.data.0.reader(),
        }
        .map_err(|err| io::Error::other(err))
        .expect("reader exists")
    }

    /// An AsyncSliceReader for the outboard file.
    ///
    /// The outboard file is used to validate the data file. It is not guaranteed
    /// to be complete.
    pub fn outboard_reader(&self) -> OutboardReader {
        OutboardReader(self.clone())
    }

    /// The most precise known total size of the data file.
    pub fn current_size(&self) -> io::Result<u64> {
        match self.storage.read().unwrap().deref() {
            BaoFileStorage::Complete(mem) => Ok(mem.data_size()),
            BaoFileStorage::IncompleteFile(file) => file.current_size(),
        }
    }

    /// The outboard for the file.
    pub fn outboard(&self) -> io::Result<PreOrderOutboard<OutboardReader>> {
        let root = self.hash.into();
        let tree = BaoTree::new(self.current_size()?, IROH_BLOCK_SIZE);
        let outboard = self.outboard_reader();
        Ok(PreOrderOutboard {
            root,
            tree,
            data: outboard,
        })
    }

    /// The hash of the file.
    pub fn hash(&self) -> Hash {
        self.hash
    }

    /// Create a new writer from the handle.
    pub fn writer(&self) -> BaoFileWriter {
        BaoFileWriter(self.clone())
    }

    /// This is the synchronous impl for writing a batch.
    fn write_batch(&self, size: u64, batch: &[BaoContentItem]) -> Result<()> {
        let mut storage = self.storage.write().unwrap();
        match storage.deref_mut() {
            BaoFileStorage::IncompleteFile(file) => {
                // already in file mode, just write the batch
                file.write_batch(size, batch)?;
                Ok(())
            }
            BaoFileStorage::Complete(_) => {
                // we are complete, so just ignore the write
                // unless there is a bug, this would just write the exact same data
                Ok(())
            }
        }
    }

    /// Downgrade to a weak reference.
    pub fn downgrade(&self) -> BaoFileHandleWeak {
        BaoFileHandleWeak(Arc::downgrade(&self.0))
    }
}

/// This is finally the thing for which we can implement BaoPairMut.
///
/// It is a BaoFileHandle wrapped in an Option, so that we can take it out
/// in the future.
#[derive(Debug)]
pub struct BaoFileWriter(BaoFileHandle);

impl BaoBatchWriter for BaoFileWriter {
    async fn write_batch(&mut self, size: u64, batch: Vec<BaoContentItem>) -> std::io::Result<()> {
        self.0
            .write_batch(size, &batch)
            .map_err(|err| io::Error::other(err))
    }

    async fn sync(&mut self) -> io::Result<()> {
        unimplemented!()
    }
}
