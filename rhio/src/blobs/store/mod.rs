//! redb backed storage
//!
//! Data can get into the store in two ways:
//!
//! 1. import from local data
//! 2. sync from a remote
//!
//! These two cases are very different. In the first case, we have the data
//! completely and don't know the hash yet. We compute the outboard and hash,
//! and only then move/reference the data into the store.
//!
//! The entry for the hash comes into existence already complete.
//!
//! In the second case, we know the hash, but don't have the data yet. We create
//! a partial entry, and then request the data from the remote. This is the more
//! complex case.
//!
//! Partial entries always start as pure in memory entries without a database
//! entry. Only once we receive enough data, we convert them into a persistent
//! partial entry. This is necessary because we can't trust the size given
//! by the remote side before receiving data. It is also an optimization,
//! because for small blobs it is not worth it to create a partial entry.
//!
//! A persistent partial entry is always stored as three files in the file
//! system: The data file, the outboard file, and a sizes file that contains
//! the most up to date information about the size of the data.
//!
//! The redb database entry for a persistent partial entry does not contain
//! any information about the size of the data until the size is exactly known.
//!
//! Updating this information on each write would be too costly.
//!
//! Marking a partial entry as complete is done from the outside. At this point
//! the size is taken as validated. Depending on the size we decide whether to
//! store data and outboard inline or to keep storing it in external files.
//!
//! Data can get out of the store in two ways:
//!
//! 1. the data and outboard of both partial and complete entries can be read at any time and
//!    shared over the network. Only data that is complete will be shared, everything else will
//!    lead to validation errors.
//!
//! 2. entries can be exported to the file system. This currently only works for complete entries.
//!
//! Tables:
//!
//! The blobs table contains a mapping from hash to rough entry state.
//! The inline_data table contains the actual data for complete entries.
//! The inline_outboard table contains the actual outboard for complete entries.
//! The tags table contains a mapping from tag to hash.
//!
//! Design:
//!
//! The redb store is accessed in a single threaded way by an actor that runs
//! on its own std thread. Communication with this actor is via a flume channel,
//! with oneshot channels for the return values if needed.
//!
//! Errors:
//!
//! ActorError is an enum containing errors that can happen inside message
//! handlers of the actor. This includes various redb related errors and io
//! errors when reading or writing non-inlined data or outboard files.
//!
//! OuterError is an enum containing all the actor errors and in addition
//! errors when communicating with the actor.
use std::{
    collections::{BTreeMap, BTreeSet},
    fs::File,
    future::Future,
    io,
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, SystemTime},
};

use bytes::Bytes;
use futures_lite::Stream;
use iroh_base::hash::{BlobFormat, Hash, HashAndFormat};
use iroh_blobs::store::{
    bao_tree::io::fsm::Outboard, DbIter, GcConfig, MapEntry, MapEntryMut, MapMut, Store,
};
use iroh_io::AsyncSliceReader;
use redb::{AccessGuard, DatabaseError, ReadableTable, StorageError};
use s3::Bucket;
use s3_file::S3File;
use serde::{Deserialize, Serialize};
use smallvec::SmallVec;

mod bao_file;
mod s3_file;
mod tables;
mod util;

use bao_file::{BaoFileConfig, BaoFileHandle, BaoFileHandleWeak};
use tables::{ReadOnlyTables, ReadableTables, Tables};

use self::util::PeekableFlumeReceiver;
use iroh_blobs::{
    store::{
        bao_tree::BaoTree, BaoBatchWriter, BaoBlobSize, ConsistencyCheckProgress, EntryStatus,
        ExportMode, ExportProgressCb, ImportMode, ImportProgress, Map, ReadableStore,
    },
    IROH_BLOCK_SIZE,
};
use iroh_blobs::{
    util::progress::{BoxedProgressSender, IdGenerator, ProgressSendError, ProgressSender},
    Tag, TempTag,
};

/// Location of the data.
///
/// Data can be inlined in the database, a file conceptually owned by the store,
/// or a number of external files conceptually owned by the user.
///
/// Only complete data can be inlined.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct DataLocation(u64);

/// The information about an entry that we keep in the entry table for quick access.
///
/// The exact info to store here is TBD, so usually you should use the accessor methods.
#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) enum EntryState {
    /// For a complete entry we always know the size. It does not make much sense
    /// to write to a complete entry, so they are much easier to share.
    Complete {
        /// Location of the data.
        data_location: DataLocation,
    },
    /// Partial entries are entries for which we know the hash, but don't have
    /// all the data. They are created when syncing from somewhere else by hash.
    ///
    /// As such they are always owned. There is also no inline storage for them.
    /// Non short lived partial entries always live in the file system, and for
    /// short lived ones we never create a database entry in the first place.
    Partial {
        /// Once we get the last chunk of a partial entry, we have validated
        /// the size of the entry despite it still being incomplete.
        ///
        /// E.g. a giant file where we just requested the last chunk.
        size: Option<u64>,
    },
}

impl Default for EntryState {
    fn default() -> Self {
        Self::Partial { size: None }
    }
}

impl EntryState {
    fn union(self, that: Self) -> ActorResult<Self> {
        match (self, that) {
            (
                Self::Complete { data_location },
                Self::Complete {
                    data_location: b_data_location,
                    ..
                },
            ) => Ok(Self::Complete {
                // combine external paths if needed
                // @TODO: I don't believe we need to do this
                // data_location: data_location.union(b_data_location)?,
                data_location,
            }),
            (a @ Self::Complete { .. }, Self::Partial { .. }) =>
            // complete wins over partial
            {
                Ok(a)
            }
            (Self::Partial { .. }, b @ Self::Complete { .. }) =>
            // complete wins over partial
            {
                Ok(b)
            }
            (Self::Partial { size: a_size }, Self::Partial { size: b_size }) =>
            // keep known size from either entry
            {
                let size = match (a_size, b_size) {
                    (Some(a_size), Some(b_size)) => {
                        // validated sizes are different. this means that at
                        // least one validation was wrong, which would be a bug
                        // in bao-tree.
                        if a_size != b_size {
                            return Err(ActorError::Inconsistent(format!(
                                "validated size mismatch {} {}",
                                a_size, b_size
                            )));
                        }
                        Some(a_size)
                    }
                    (Some(a_size), None) => Some(a_size),
                    (None, Some(b_size)) => Some(b_size),
                    (None, None) => None,
                };
                Ok(Self::Partial { size })
            }
        }
    }
}

impl redb::Value for EntryState {
    type SelfType<'a> = EntryState;

    type AsBytes<'a> = SmallVec<[u8; 128]>;

    fn fixed_width() -> Option<usize> {
        None
    }

    fn from_bytes<'a>(data: &'a [u8]) -> Self::SelfType<'a>
    where
        Self: 'a,
    {
        postcard::from_bytes(data).unwrap()
    }

    fn as_bytes<'a, 'b: 'a>(value: &'a Self::SelfType<'b>) -> Self::AsBytes<'a>
    where
        Self: 'a,
        Self: 'b,
    {
        postcard::to_extend(value, SmallVec::new()).unwrap()
    }

    fn type_name() -> redb::TypeName {
        redb::TypeName::new("EntryState")
    }
}

/// Options for directories used by the file store.
#[derive(Debug, Clone)]
pub struct PathOptions {
    pub outboard_path: PathBuf,
    /// Path to the directory where temp files are stored.
    /// This *must* be on the same device as `data_path`, since we need to
    /// atomically move temp files into place.
    pub temp_path: PathBuf,
}

impl PathOptions {
    fn new(root: &Path) -> Self {
        Self {
            outboard_path: root.join("data"),
            temp_path: root.join("temp"),
        }
    }

    fn owned_outboard_path(&self, hash: &Hash) -> PathBuf {
        self.outboard_path.join(format!("{}.obao4", hash.to_hex()))
    }

    fn owned_sizes_path(&self, hash: &Hash) -> PathBuf {
        self.outboard_path.join(format!("{}.sizes4", hash.to_hex()))
    }

    fn temp_file_name(&self) -> PathBuf {
        unimplemented!()
        // self.temp_path.join(temp_name())
    }
}

/// Options for transaction batching.
#[derive(Debug, Clone)]
pub struct BatchOptions {
    /// Maximum number of actor messages to batch before creating a new read transaction.
    pub max_read_batch: usize,
    /// Maximum duration to wait before committing a read transaction.
    pub max_read_duration: Duration,
    /// Maximum number of actor messages to batch before committing write transaction.
    pub max_write_batch: usize,
    /// Maximum duration to wait before committing a write transaction.
    pub max_write_duration: Duration,
}

impl Default for BatchOptions {
    fn default() -> Self {
        Self {
            max_read_batch: 10000,
            max_read_duration: Duration::from_secs(1),
            max_write_batch: 1000,
            max_write_duration: Duration::from_millis(500),
        }
    }
}

/// Options for the file store.
#[derive(Debug, Clone)]
pub struct Options {
    /// Path options.
    pub path: PathOptions,
    /// Transaction batching options.
    pub batch: BatchOptions,
}

/// Use BaoFileHandle as the entry type for the map.
pub type Entry = BaoFileHandle;

impl MapEntry for Entry {
    fn hash(&self) -> Hash {
        self.hash()
    }

    fn size(&self) -> BaoBlobSize {
        let size = self.current_size().unwrap();
        tracing::trace!("redb::Entry::size() = {}", size);
        BaoBlobSize::new(size, self.is_complete())
    }

    fn is_complete(&self) -> bool {
        self.is_complete()
    }

    async fn outboard(&self) -> io::Result<impl Outboard> {
        self.outboard()
    }

    async fn data_reader(&self) -> io::Result<impl AsyncSliceReader> {
        Ok(self.data_reader())
    }
}

impl MapEntryMut for Entry {
    async fn batch_writer(&self) -> io::Result<impl BaoBatchWriter> {
        Ok(self.writer())
    }
}

#[derive(derive_more::Debug)]
pub(crate) enum ActorMessage {
    // Query method: get a file handle for a hash, if it exists.
    // This will produce a file handle even for entries that are not yet in redb at all.
    Get {
        hash: Hash,
        tx: oneshot::Sender<ActorResult<Option<BaoFileHandle>>>,
    },
    /// Query method: get the rough entry status for a hash. Just complete, partial or not found.
    EntryStatus {
        hash: Hash,
        tx: oneshot::Sender<ActorResult<EntryStatus>>,
    },
    /// Modification method: get or create a file handle for a hash.
    ///
    /// If the entry exists in redb, either partial or complete, the corresponding
    /// data will be returned. If it does not yet exist, a new partial file handle
    /// will be created, but not yet written to redb.
    GetOrCreate {
        hash: Hash,
        tx: oneshot::Sender<ActorResult<BaoFileHandle>>,
    },
    /// Modification method: marks a partial entry as complete.
    /// Calling this on a complete entry is a no-op.
    OnComplete { handle: BaoFileHandle },
    /// Bulk query method: get entries from the blobs table
    Blobs {
        #[debug(skip)]
        filter: FilterPredicate<Hash, EntryState>,
        #[allow(clippy::type_complexity)]
        tx: oneshot::Sender<
            ActorResult<Vec<std::result::Result<(Hash, EntryState), StorageError>>>,
        >,
    },
    /// Bulk query method: get the entire tags table
    Tags {
        #[debug(skip)]
        filter: FilterPredicate<Tag, HashAndFormat>,
        #[allow(clippy::type_complexity)]
        tx: oneshot::Sender<
            ActorResult<Vec<std::result::Result<(Tag, HashAndFormat), StorageError>>>,
        >,
    },
    /// Modification method: set a tag to a value, or remove it.
    SetTag {
        tag: Tag,
        value: Option<HashAndFormat>,
        tx: oneshot::Sender<ActorResult<()>>,
    },
    /// Modification method: create a new unique tag and set it to a value.
    CreateTag {
        hash: HashAndFormat,
        tx: oneshot::Sender<ActorResult<Tag>>,
    },
    /// Sync the entire database to disk.
    ///
    /// This just makes sure that there is no write transaction open.
    Sync { tx: oneshot::Sender<()> },
    /// Internal method: shutdown the actor.
    ///
    /// Can have an optional oneshot sender to signal when the actor has shut down.
    Shutdown { tx: Option<oneshot::Sender<()>> },
}
impl ActorMessage {
    fn category(&self) -> MessageCategory {
        match self {
            Self::Get { .. }
            | Self::GetOrCreate { .. }
            | Self::EntryStatus { .. }
            | Self::Blobs { .. }
            | Self::Tags { .. } => MessageCategory::ReadOnly,
            Self::OnComplete { .. } | Self::SetTag { .. } | Self::CreateTag { .. } => {
                MessageCategory::ReadWrite
            }
            Self::Sync { .. } | Self::Shutdown { .. } => MessageCategory::TopLevel,
        }
    }
}

enum MessageCategory {
    ReadOnly,
    ReadWrite,
    TopLevel,
}

/// Predicate for filtering entries in a redb table.
pub(crate) type FilterPredicate<K, V> =
    Box<dyn Fn(u64, AccessGuard<K>, AccessGuard<V>) -> Option<(K, V)> + Send + Sync>;

/// Storage that is using a redb database for small files and files for
/// large files.
#[derive(Debug, Clone)]
pub struct S3Store(Arc<StoreInner>);

impl S3Store {
    /// Load or create a new store.
    pub async fn load(root: impl AsRef<Path>) -> io::Result<Self> {
        let path = root.as_ref();
        let db_path = path.join("blobs.db");
        let options = Options {
            path: PathOptions::new(path),
            batch: Default::default(),
        };
        Self::new(db_path, options).await
    }

    /// Create a new store with custom options.
    pub async fn new(path: PathBuf, options: Options) -> io::Result<Self> {
        // spawn_blocking because StoreInner::new creates directories
        let rt = tokio::runtime::Handle::try_current()
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "no tokio runtime"))?;
        let inner =
            tokio::task::spawn_blocking(move || StoreInner::new_sync(path, options, rt)).await??;
        Ok(Self(Arc::new(inner)))
    }
}

#[derive(Debug)]
struct StoreInner {
    tx: async_channel::Sender<ActorMessage>,
    // temp: Arc<RwLock<TempCounterMap>>,
    handle: Option<std::thread::JoinHandle<()>>,
    path_options: Arc<PathOptions>,
}

impl StoreInner {
    fn new_sync(path: PathBuf, options: Options, rt: tokio::runtime::Handle) -> io::Result<Self> {
        // tracing::trace!(
        //     "creating data directory: {}",
        //     options.path.data_path.display()
        // );
        // std::fs::create_dir_all(&options.path.data_path)?;
        tracing::trace!(
            "creating temp directory: {}",
            options.path.temp_path.display()
        );
        std::fs::create_dir_all(&options.path.temp_path)?;
        tracing::trace!(
            "creating parent directory for db file{}",
            path.parent().unwrap().display()
        );
        std::fs::create_dir_all(path.parent().unwrap())?;
        // let temp: Arc<RwLock<TempCounterMap>> = Default::default();
        let (actor, tx) = Actor::new(&path, options.clone(), rt.clone())?;
        let handle = std::thread::Builder::new()
            .name("redb-actor".to_string())
            .spawn(move || {
                rt.block_on(async move {
                    if let Err(cause) = actor.run_batched().await {
                        tracing::error!("redb actor failed: {}", cause);
                    }
                });
            })
            .expect("failed to spawn thread");
        Ok(Self {
            tx,
            // temp,
            handle: Some(handle),
            path_options: Arc::new(options.path),
        })
    }

    pub async fn get(&self, hash: Hash) -> OuterResult<Option<BaoFileHandle>> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(ActorMessage::Get { hash, tx }).await?;
        Ok(rx.await??)
    }

    async fn get_or_create(&self, hash: Hash) -> OuterResult<BaoFileHandle> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(ActorMessage::GetOrCreate { hash, tx }).await?;
        Ok(rx.await??)
    }

    async fn blobs(&self) -> OuterResult<Vec<io::Result<Hash>>> {
        let (tx, rx) = oneshot::channel();
        let filter: FilterPredicate<Hash, EntryState> = Box::new(|_i, k, v| {
            let v = v.value();
            if let EntryState::Complete { .. } = &v {
                Some((k.value(), v))
            } else {
                None
            }
        });
        self.tx.send(ActorMessage::Blobs { filter, tx }).await?;
        let blobs = rx.await?;
        let res = blobs?
            .into_iter()
            .map(|r| {
                r.map(|(hash, _)| hash)
                    .map_err(|e| ActorError::from(e).into())
            })
            .collect::<Vec<_>>();
        Ok(res)
    }

    async fn partial_blobs(&self) -> OuterResult<Vec<io::Result<Hash>>> {
        let (tx, rx) = oneshot::channel();
        let filter: FilterPredicate<Hash, EntryState> = Box::new(|_i, k, v| {
            let v = v.value();
            if let EntryState::Partial { .. } = &v {
                Some((k.value(), v))
            } else {
                None
            }
        });
        self.tx.send(ActorMessage::Blobs { filter, tx }).await?;
        let blobs = rx.await?;
        let res = blobs?
            .into_iter()
            .map(|r| {
                r.map(|(hash, _)| hash)
                    .map_err(|e| ActorError::from(e).into())
            })
            .collect::<Vec<_>>();
        Ok(res)
    }

    async fn tags(&self) -> OuterResult<Vec<io::Result<(Tag, HashAndFormat)>>> {
        let (tx, rx) = oneshot::channel();
        let filter: FilterPredicate<Tag, HashAndFormat> =
            Box::new(|_i, k, v| Some((k.value(), v.value())));
        self.tx.send(ActorMessage::Tags { filter, tx }).await?;
        let tags = rx.await?;
        // transform the internal error type into io::Error
        let tags = tags?
            .into_iter()
            .map(|r| r.map_err(|e| ActorError::from(e).into()))
            .collect();
        Ok(tags)
    }

    async fn set_tag(&self, tag: Tag, value: Option<HashAndFormat>) -> OuterResult<()> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(ActorMessage::SetTag { tag, value, tx })
            .await?;
        Ok(rx.await??)
    }

    async fn create_tag(&self, hash: HashAndFormat) -> OuterResult<Tag> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(ActorMessage::CreateTag { hash, tx }).await?;
        Ok(rx.await??)
    }

    async fn entry_status(&self, hash: &Hash) -> OuterResult<EntryStatus> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(ActorMessage::EntryStatus { hash: *hash, tx })
            .await?;
        Ok(rx.await??)
    }

    fn entry_status_sync(&self, hash: &Hash) -> OuterResult<EntryStatus> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send_blocking(ActorMessage::EntryStatus { hash: *hash, tx })?;
        Ok(rx.recv()??)
    }

    async fn complete(&self, entry: Entry) -> OuterResult<()> {
        self.tx
            .send(ActorMessage::OnComplete { handle: entry })
            .await?;
        Ok(())
    }

    async fn sync(&self) -> OuterResult<()> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(ActorMessage::Sync { tx }).await?;
        Ok(rx.await?)
    }

    fn temp_file_name(&self) -> PathBuf {
        self.path_options.temp_file_name()
    }

    async fn shutdown(&self) {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(ActorMessage::Shutdown { tx: Some(tx) })
            .await
            .ok();
        rx.await.ok();
    }
}

impl Drop for StoreInner {
    fn drop(&mut self) {
        if let Some(handle) = self.handle.take() {
            self.tx
                .send_blocking(ActorMessage::Shutdown { tx: None })
                .ok();
            handle.join().ok();
        }
    }
}

struct ActorState {
    handles: BTreeMap<Hash, BaoFileHandleWeak>,
    protected: BTreeSet<Hash>,
    // temp: Arc<RwLock<TempCounterMap>>,
    msgs_rx: async_channel::Receiver<ActorMessage>,
    create_options: Arc<BaoFileConfig>,
    options: Options,
    rt: tokio::runtime::Handle,
}

/// The actor for the redb store.
///
/// It is split into the database and the rest of the state to allow for split
/// borrows in the message handlers.
struct Actor {
    db: redb::Database,
    state: ActorState,
}

/// Error type for message handler functions of the redb actor.
///
/// What can go wrong are various things with redb, as well as io errors related
/// to files other than redb.
#[derive(Debug, thiserror::Error)]
pub(crate) enum ActorError {
    #[error("table error: {0}")]
    Table(#[from] redb::TableError),
    #[error("database error: {0}")]
    Database(#[from] redb::DatabaseError),
    #[error("transaction error: {0}")]
    Transaction(#[from] redb::TransactionError),
    #[error("commit error: {0}")]
    Commit(#[from] redb::CommitError),
    #[error("storage error: {0}")]
    Storage(#[from] redb::StorageError),
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    #[error("inconsistent database state: {0}")]
    Inconsistent(String),
    #[error("error during database migration: {0}")]
    Migration(#[source] anyhow::Error),
}

impl From<ActorError> for io::Error {
    fn from(e: ActorError) -> Self {
        match e {
            ActorError::Io(e) => e,
            e => io::Error::new(io::ErrorKind::Other, e),
        }
    }
}

/// Result type for handler functions of the redb actor.
///
/// See [`ActorError`] for what can go wrong.
pub(crate) type ActorResult<T> = std::result::Result<T, ActorError>;

/// Error type for calling the redb actor from the store.
///
/// What can go wrong is all the things in [`ActorError`] and in addition
/// sending and receiving messages.
#[derive(Debug, thiserror::Error)]
pub(crate) enum OuterError {
    #[error("inner error: {0}")]
    Inner(#[from] ActorError),
    #[error("send error")]
    Send,
    #[error("progress send error: {0}")]
    ProgressSend(#[from] ProgressSendError),
    #[error("recv error: {0}")]
    Recv(#[from] oneshot::RecvError),
    #[error("recv error: {0}")]
    AsyncChannelRecv(#[from] async_channel::RecvError),
    #[error("join error: {0}")]
    JoinTask(#[from] tokio::task::JoinError),
}

impl From<async_channel::SendError<ActorMessage>> for OuterError {
    fn from(_e: async_channel::SendError<ActorMessage>) -> Self {
        OuterError::Send
    }
}

/// Result type for calling the redb actor from the store.
///
/// See [`OuterError`] for what can go wrong.
pub(crate) type OuterResult<T> = std::result::Result<T, OuterError>;

impl From<io::Error> for OuterError {
    fn from(e: io::Error) -> Self {
        OuterError::Inner(ActorError::Io(e))
    }
}

impl From<OuterError> for io::Error {
    fn from(e: OuterError) -> Self {
        match e {
            OuterError::Inner(ActorError::Io(e)) => e,
            e => io::Error::new(io::ErrorKind::Other, e),
        }
    }
}

impl Map for S3Store {
    type Entry = Entry;

    async fn get(&self, hash: &Hash) -> io::Result<Option<Self::Entry>> {
        Ok(self.0.get(*hash).await?.map(From::from))
    }
}

impl MapMut for S3Store {
    type EntryMut = Entry;

    async fn get_or_create(&self, hash: Hash, _size: u64) -> io::Result<Self::EntryMut> {
        Ok(self.0.get_or_create(hash).await?)
    }

    async fn entry_status(&self, hash: &Hash) -> io::Result<EntryStatus> {
        Ok(self.0.entry_status(hash).await?)
    }

    async fn get_mut(&self, hash: &Hash) -> io::Result<Option<Self::EntryMut>> {
        self.get(hash).await
    }

    async fn insert_complete(&self, entry: Self::EntryMut) -> io::Result<()> {
        Ok(self.0.complete(entry).await?)
    }

    fn entry_status_sync(&self, hash: &Hash) -> io::Result<EntryStatus> {
        Ok(self.0.entry_status_sync(hash)?)
    }
}

impl ReadableStore for S3Store {
    async fn blobs(&self) -> io::Result<DbIter<Hash>> {
        Ok(Box::new(self.0.blobs().await?.into_iter()))
    }

    async fn partial_blobs(&self) -> io::Result<DbIter<Hash>> {
        Ok(Box::new(self.0.partial_blobs().await?.into_iter()))
    }

    async fn tags(&self) -> io::Result<DbIter<(Tag, HashAndFormat)>> {
        Ok(Box::new(self.0.tags().await?.into_iter()))
    }

    fn temp_tags(&self) -> Box<dyn Iterator<Item = HashAndFormat> + Send + Sync + 'static> {
        // Box::new(self.0.temp.read().unwrap().keys())
        unimplemented!()
    }

    async fn consistency_check(
        &self,
        repair: bool,
        tx: BoxedProgressSender<ConsistencyCheckProgress>,
    ) -> io::Result<()> {
        unimplemented!()
    }

    async fn export(
        &self,
        hash: Hash,
        target: PathBuf,
        mode: ExportMode,
        progress: ExportProgressCb,
    ) -> io::Result<()> {
        unimplemented!()
    }
}

impl Store for S3Store {
    async fn import_file(
        &self,
        path: PathBuf,
        mode: ImportMode,
        format: BlobFormat,
        progress: impl ProgressSender<Msg = ImportProgress> + IdGenerator,
    ) -> io::Result<(TempTag, u64)> {
        unimplemented!()
    }

    async fn import_bytes(
        &self,
        data: bytes::Bytes,
        format: iroh_base::hash::BlobFormat,
    ) -> io::Result<TempTag> {
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

    async fn set_tag(&self, name: Tag, hash: Option<HashAndFormat>) -> io::Result<()> {
        Ok(self.0.set_tag(name, hash).await?)
    }

    async fn create_tag(&self, hash: HashAndFormat) -> io::Result<Tag> {
        Ok(self.0.create_tag(hash).await?)
    }

    async fn delete(&self, hashes: Vec<Hash>) -> io::Result<()> {
        unimplemented!()
    }

    async fn gc_run<G, Gut>(&self, config: GcConfig, protected_cb: G)
    where
        G: Fn() -> Gut,
        Gut: Future<Output = BTreeSet<Hash>> + Send,
    {
        unimplemented!()
    }

    fn temp_tag(&self, value: HashAndFormat) -> TempTag {
        // self.0.temp.temp_tag(value)
        unimplemented!()
    }

    async fn sync(&self) -> io::Result<()> {
        Ok(self.0.sync().await?)
    }

    async fn shutdown(&self) {
        self.0.shutdown().await;
    }
}

impl Actor {
    fn new(
        path: &Path,
        options: Options,
        // temp: Arc<RwLock<TempCounterMap>>,
        rt: tokio::runtime::Handle,
    ) -> ActorResult<(Self, async_channel::Sender<ActorMessage>)> {
        let db = match redb::Database::create(path) {
            Ok(db) => db,
            Err(DatabaseError::UpgradeRequired(1)) => {
                unimplemented!();
                // migrate_redb_v1_v2::run(path).map_err(ActorError::Migration)?
            }
            Err(err) => return Err(err.into()),
        };

        let txn = db.begin_write()?;
        // create tables and drop them just to create them.
        let tables = Tables::new(&txn)?;
        drop(tables);
        txn.commit()?;
        // make the channel relatively large. there are some messages that don't
        // require a response, it's fine if they pile up a bit.
        let (tx, rx) = async_channel::bounded(1024);
        // let tx2 = tx.clone();
        let create_options = BaoFileConfig::new(
            Arc::new(options.path.outboard_path.clone()),
            // @TODO: Pass bucket and actual data prefix into method
            Bucket::new_public("name", s3::Region::EuNorth1).expect("valid bucket"),
        );
        Ok((
            Self {
                db,
                state: ActorState {
                    // temp,
                    handles: BTreeMap::new(),
                    protected: BTreeSet::new(),
                    msgs_rx: rx,
                    options,
                    create_options: Arc::new(create_options),
                    rt,
                },
            },
            tx,
        ))
    }

    async fn run_batched(mut self) -> ActorResult<()> {
        let mut msgs = PeekableFlumeReceiver::new(self.state.msgs_rx.clone());
        while let Some(msg) = msgs.recv().await {
            if let ActorMessage::Shutdown { tx } = msg {
                // Make sure the database is dropped before we send the reply.
                drop(self);
                if let Some(tx) = tx {
                    tx.send(()).ok();
                }
                break;
            }
            match msg.category() {
                MessageCategory::TopLevel => {
                    self.state.handle_toplevel(&self.db, msg)?;
                }
                MessageCategory::ReadOnly => {
                    msgs.push_back(msg).expect("just recv'd");
                    tracing::debug!("starting read transaction");
                    let txn = self.db.begin_read()?;
                    let tables = ReadOnlyTables::new(&txn)?;
                    let count = self.state.options.batch.max_read_batch;
                    let timeout = tokio::time::sleep(self.state.options.batch.max_read_duration);
                    tokio::pin!(timeout);
                    for _ in 0..count {
                        tokio::select! {
                            msg = msgs.recv() => {
                                if let Some(msg) = msg {
                                    if let Err(msg) = self.state.handle_readonly(&tables, msg)? {
                                        msgs.push_back(msg).expect("just recv'd");
                                        break;
                                    }
                                } else {
                                    break;
                                }
                            }
                            _ = &mut timeout => {
                                tracing::debug!("read transaction timed out");
                                break;
                            }
                        }
                    }
                    tracing::debug!("done with read transaction");
                }
                MessageCategory::ReadWrite => {
                    msgs.push_back(msg).expect("just recv'd");
                    tracing::debug!("starting write transaction");
                    let txn = self.db.begin_write()?;
                    let mut tables = Tables::new(&txn)?;
                    let count = self.state.options.batch.max_write_batch;
                    let timeout = tokio::time::sleep(self.state.options.batch.max_write_duration);
                    tokio::pin!(timeout);
                    for _ in 0..count {
                        tokio::select! {
                            msg = msgs.recv() => {
                                if let Some(msg) = msg {
                                    if let Err(msg) = self.state.handle_readwrite(&mut tables, msg)? {
                                        msgs.push_back(msg).expect("just recv'd");
                                        break;
                                    }
                                } else {
                                    break;
                                }
                            }
                            _ = &mut timeout => {
                                tracing::debug!("write transaction timed out");
                                break;
                            }
                        }
                    }
                    drop(tables);
                    txn.commit()?;
                    tracing::debug!("write transaction committed");
                }
            }
        }
        tracing::debug!("redb actor done");
        Ok(())
    }
}

impl ActorState {
    fn entry_status(
        &mut self,
        tables: &impl ReadableTables,
        hash: Hash,
    ) -> ActorResult<EntryStatus> {
        let status = match tables.blobs().get(hash)? {
            Some(guard) => match guard.value() {
                EntryState::Complete { .. } => EntryStatus::Complete,
                EntryState::Partial { .. } => EntryStatus::Partial,
            },
            None => EntryStatus::NotFound,
        };
        Ok(status)
    }

    fn get(
        &mut self,
        tables: &impl ReadableTables,
        hash: Hash,
    ) -> ActorResult<Option<BaoFileHandle>> {
        if let Some(handle) = self.handles.get(&hash).and_then(|weak| weak.upgrade()) {
            return Ok(Some(handle));
        }
        let Some(entry) = tables.blobs().get(hash)? else {
            return Ok(None);
        };
        // todo: if complete, load inline data and/or outboard into memory if needed,
        // and return a complete entry.
        let entry = entry.value();
        let config = self.create_options.clone();
        let handle = match entry {
            EntryState::Complete { data_location } => {
                let data = load_data(tables, data_location, &hash)?;
                let outboard = load_outboard(tables, &self.options.path, data.1, &hash)?;
                BaoFileHandle::new_complete(config, hash, data, outboard)
            }
            EntryState::Partial { .. } => BaoFileHandle::incomplete_file(config, hash)?,
        };
        self.handles.insert(hash, handle.downgrade());
        Ok(Some(handle))
    }

    fn get_or_create(
        &mut self,
        tables: &impl ReadableTables,
        hash: Hash,
    ) -> ActorResult<BaoFileHandle> {
        self.protected.insert(hash);
        if let Some(handle) = self.handles.get(&hash).and_then(|x| x.upgrade()) {
            return Ok(handle);
        }
        let entry = tables.blobs().get(hash)?;
        let handle = if let Some(entry) = entry {
            let entry = entry.value();
            match entry {
                EntryState::Complete { data_location, .. } => {
                    let data = load_data(tables, data_location, &hash)?;
                    let outboard = load_outboard(tables, &self.options.path, data.1, &hash)?;
                    tracing::debug!("creating complete entry for {}", hash.to_hex());
                    BaoFileHandle::new_complete(self.create_options.clone(), hash, data, outboard)
                }
                EntryState::Partial { .. } => {
                    tracing::debug!("creating partial entry for {}", hash.to_hex());
                    BaoFileHandle::incomplete_file(self.create_options.clone(), hash)?
                }
            }
        } else {
            // @TODO: We don't handle only in memory bao files now, should we error here or return
            // an option?
            // BaoFileHandle::incomplete_mem(self.create_options.clone(), hash)
            todo!()
        };
        self.handles.insert(hash, handle.downgrade());
        Ok(handle)
    }

    /// Read the entire blobs table. Callers can then sift through the results to find what they need
    fn blobs(
        &mut self,
        tables: &impl ReadableTables,
        filter: FilterPredicate<Hash, EntryState>,
    ) -> ActorResult<Vec<std::result::Result<(Hash, EntryState), StorageError>>> {
        let mut res = Vec::new();
        let mut index = 0u64;
        #[allow(clippy::explicit_counter_loop)]
        for item in tables.blobs().iter()? {
            match item {
                Ok((k, v)) => {
                    if let Some(item) = filter(index, k, v) {
                        res.push(Ok(item));
                    }
                }
                Err(e) => {
                    res.push(Err(e));
                }
            }
            index += 1;
        }
        Ok(res)
    }

    /// Read the entire tags table. Callers can then sift through the results to find what they need
    fn tags(
        &mut self,
        tables: &impl ReadableTables,
        filter: FilterPredicate<Tag, HashAndFormat>,
    ) -> ActorResult<Vec<std::result::Result<(Tag, HashAndFormat), StorageError>>> {
        let mut res = Vec::new();
        let mut index = 0u64;
        #[allow(clippy::explicit_counter_loop)]
        for item in tables.tags().iter()? {
            match item {
                Ok((k, v)) => {
                    if let Some(item) = filter(index, k, v) {
                        res.push(Ok(item));
                    }
                }
                Err(e) => {
                    res.push(Err(e));
                }
            }
            index += 1;
        }
        Ok(res)
    }

    fn create_tag(&mut self, tables: &mut Tables, content: HashAndFormat) -> ActorResult<Tag> {
        let tag = {
            let tag = Tag::auto(SystemTime::now(), |x| {
                matches!(tables.tags.get(Tag(Bytes::copy_from_slice(x))), Ok(Some(_)))
            });
            tables.tags.insert(tag.clone(), content)?;
            tag
        };
        Ok(tag)
    }

    fn set_tag(
        &self,
        tables: &mut Tables,
        tag: Tag,
        value: Option<HashAndFormat>,
    ) -> ActorResult<()> {
        match value {
            Some(value) => {
                tables.tags.insert(tag, value)?;
            }
            None => {
                tables.tags.remove(tag)?;
            }
        }
        Ok(())
    }

    fn on_complete(&mut self, tables: &mut Tables, entry: BaoFileHandle) -> ActorResult<()> {
        let hash = entry.hash();
        tracing::trace!("on_complete({})", hash.to_hex());

        //@TODO: assert data bytes size is correct against outboard data bytes size
        if entry.is_complete() {
            // @TODO: The entry is already complete, we don't need to do anything but should log
            // here as there must be some logic error elsewhere.
            todo!()
        }

        let data_size = entry.current_size()?;

        tracing::debug!(
            "inserting complete entry for {}, {} bytes",
            hash.to_hex(),
            data_size,
        );
        let entry = tables
            .blobs()
            .get(hash)?
            .map(|x| x.value())
            .unwrap_or_default();
        let entry = entry.union(EntryState::Complete {
            data_location: DataLocation(data_size),
        })?;
        tables.blobs.insert(hash, entry)?;
        Ok(())
    }

    fn handle_toplevel(&mut self, db: &redb::Database, msg: ActorMessage) -> ActorResult<()> {
        match msg {
            ActorMessage::Sync { tx } => {
                tx.send(()).ok();
            }
            x => {
                return Err(ActorError::Inconsistent(format!(
                    "unexpected message for handle_toplevel: {:?}",
                    x
                )))
            }
        }
        Ok(())
    }

    fn handle_readonly(
        &mut self,
        tables: &impl ReadableTables,
        msg: ActorMessage,
    ) -> ActorResult<std::result::Result<(), ActorMessage>> {
        match msg {
            ActorMessage::Get { hash, tx } => {
                let res = self.get(tables, hash);
                tx.send(res).ok();
            }
            ActorMessage::GetOrCreate { hash, tx } => {
                let res = self.get_or_create(tables, hash);
                tx.send(res).ok();
            }
            ActorMessage::EntryStatus { hash, tx } => {
                let res = self.entry_status(tables, hash);
                tx.send(res).ok();
            }
            ActorMessage::Blobs { filter, tx } => {
                let res = self.blobs(tables, filter);
                tx.send(res).ok();
            }
            ActorMessage::Tags { filter, tx } => {
                let res = self.tags(tables, filter);
                tx.send(res).ok();
            }
            x => return Ok(Err(x)),
        }
        Ok(Ok(()))
    }

    fn handle_readwrite(
        &mut self,
        tables: &mut Tables,
        msg: ActorMessage,
    ) -> ActorResult<std::result::Result<(), ActorMessage>> {
        match msg {
            ActorMessage::SetTag { tag, value, tx } => {
                let res = self.set_tag(tables, tag, value);
                tx.send(res).ok();
            }
            ActorMessage::CreateTag { hash, tx } => {
                let res = self.create_tag(tables, hash);
                tx.send(res).ok();
            }
            ActorMessage::OnComplete { handle } => {
                let res = self.on_complete(tables, handle);
                res.ok();
            }
            msg => {
                // try to handle it as readonly
                if let Err(msg) = self.handle_readonly(tables, msg)? {
                    return Ok(Err(msg));
                }
            }
        }
        Ok(Ok(()))
    }
}

fn load_data(
    tables: &impl ReadableTables,
    location: DataLocation,
    hash: &Hash,
) -> ActorResult<(S3File, u64)> {
    unimplemented!()
    // Ok((file, location.0))
}

fn load_outboard(
    tables: &impl ReadableTables,
    options: &PathOptions,
    size: u64,
    hash: &Hash,
) -> ActorResult<(File, u64)> {
    let outboard_size = BaoTree::new(size, IROH_BLOCK_SIZE).outboard_size();
    let path = options.owned_outboard_path(hash);
    let Ok(file) = std::fs::File::open(&path) else {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!("file not found: {} size={}", path.display(), outboard_size),
        )
        .into());
    };
    Ok((file, outboard_size))
}
