//! minio and filesystem backed storage
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    fs::File,
    future::Future,
    io,
    path::PathBuf,
    sync::Arc,
};

use bytes::Bytes;
use futures_lite::Stream;
use iroh_base::hash::{BlobFormat, Hash, HashAndFormat};
use iroh_blobs::store::bao_tree::io::fsm::Outboard;
use iroh_blobs::store::bao_tree::BaoTree;
use iroh_blobs::store::{
    BaoBatchWriter, BaoBlobSize, ConsistencyCheckProgress, DbIter, EntryStatus, ExportMode,
    ExportProgressCb, GcConfig, ImportMode, ImportProgress, Map, MapEntry, MapEntryMut, MapMut,
    ReadableStore, Store,
};
use iroh_blobs::util::progress::{
    BoxedProgressSender, IdGenerator, ProgressSendError, ProgressSender,
};
use iroh_blobs::{Tag, TempTag, IROH_BLOCK_SIZE};
use iroh_io::AsyncSliceReader;
use s3::Bucket;
use s3_file::S3File;
use serde::{Deserialize, Serialize};
use tracing::warn;

mod bao_file;
mod s3_file;
mod util;

use self::bao_file::{BaoFileConfig, BaoFileHandle, BaoFileHandleWeak};
use self::util::PeekableFlumeReceiver;

/// The information about an entry that we keep in the entry table for quick access.
///
/// The exact info to store here is TBD, so usually you should use the accessor methods.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct EntryState {
    size: u64,

    complete: bool,

    data_path: String,
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
    CompleteBlobs {
        tx: oneshot::Sender<Vec<io::Result<Hash>>>,
    },
    IncompleteBlobs {
        tx: oneshot::Sender<Vec<io::Result<Hash>>>,
    },
    /// Bulk query method: get the entire tags table
    // Tags {
    //     #[debug(skip)]
    //     filter: FilterPredicate<Tag, HashAndFormat>,
    //     #[allow(clippy::type_complexity)]
    //     tx: oneshot::Sender<
    //         ActorResult<Vec<std::result::Result<(Tag, HashAndFormat), StorageError>>>,
    //     >,
    // },
    // /// Modification method: set a tag to a value, or remove it.
    // SetTag {
    //     tag: Tag,
    //     value: Option<HashAndFormat>,
    //     tx: oneshot::Sender<ActorResult<()>>,
    // },
    // /// Modification method: create a new unique tag and set it to a value.
    // CreateTag {
    //     hash: HashAndFormat,
    //     tx: oneshot::Sender<ActorResult<Tag>>,
    // },
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
            | Self::CompleteBlobs { .. }
            | Self::IncompleteBlobs { .. } => MessageCategory::ReadOnly,
            Self::OnComplete { .. } => MessageCategory::ReadWrite,
            Self::Sync { .. } | Self::Shutdown { .. } => MessageCategory::TopLevel,
        }
    }
}

enum MessageCategory {
    ReadOnly,
    ReadWrite,
    TopLevel,
}

/// Storage that is using a redb database for small files and files for
/// large files.
#[derive(Debug, Clone)]
pub struct S3Store(Arc<StoreInner>);

impl S3Store {
    /// Create a new store with custom options.
    pub async fn new(bucket: Bucket, path: PathBuf) -> io::Result<Self> {
        // spawn_blocking because StoreInner::new creates directories
        let rt = tokio::runtime::Handle::try_current()
            .map_err(|_| io::Error::new(io::ErrorKind::Other, "no tokio runtime"))?;
        let inner =
            tokio::task::spawn_blocking(move || StoreInner::new_sync(bucket, path, rt)).await??;
        Ok(Self(Arc::new(inner)))
    }
}

#[derive(Debug)]
struct StoreInner {
    tx: async_channel::Sender<ActorMessage>,
    // temp: Arc<RwLock<TempCounterMap>>,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl StoreInner {
    fn new_sync(bucket: Bucket, path: PathBuf, rt: tokio::runtime::Handle) -> io::Result<Self> {
        // tracing::trace!(
        //     "creating data directory: {}",
        //     outboard_path.data_path.display()
        // );
        // std::fs::create_dir_all(&outboard_path.data_path)?;
        // tracing::trace!(
        //     "creating temp directory: {}",
        //     outboard_path.temp_path.display()
        // );
        // std::fs::create_dir_all(&outboard_path.temp_path)?;
        tracing::trace!(
            "creating parent directory for db file{}",
            path.parent().unwrap().display()
        );
        std::fs::create_dir_all(path.parent().unwrap())?;
        // let temp: Arc<RwLock<TempCounterMap>> = Default::default();
        let (actor, tx) = Actor::new(bucket, path.clone())?;
        let handle = std::thread::Builder::new()
            .name("redb-actor".to_string())
            .spawn(move || {
                rt.block_on(async move {
                    if let Err(cause) = actor.run().await {
                        tracing::error!("redb actor failed: {}", cause);
                    }
                });
            })
            .expect("failed to spawn thread");
        Ok(Self {
            tx,
            // temp,
            handle: Some(handle),
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
        self.tx.send(ActorMessage::CompleteBlobs { tx }).await?;
        let blobs = rx.await?;
        Ok(blobs)
    }

    async fn partial_blobs(&self) -> OuterResult<Vec<io::Result<Hash>>> {
        let (tx, rx) = oneshot::channel();
        self.tx.send(ActorMessage::IncompleteBlobs { tx }).await?;
        let blobs = rx.await?;
        Ok(blobs)
    }

    async fn tags(&self) -> OuterResult<Vec<io::Result<(Tag, HashAndFormat)>>> {
        unimplemented!()
        // let (tx, rx) = oneshot::channel();
        // let filter: FilterPredicate<Tag, HashAndFormat> =
        //     Box::new(|_i, k, v| Some((k.value(), v.value())));
        // self.tx.send(ActorMessage::Tags { filter, tx }).await?;
        // let tags = rx.await?;
        // // transform the internal error type into io::Error
        // let tags = tags?
        //     .into_iter()
        //     .map(|r| r.map_err(|e| ActorError::from(e).into()))
        //     .collect();
        // Ok(tags)
    }

    async fn set_tag(&self, tag: Tag, value: Option<HashAndFormat>) -> OuterResult<()> {
        unimplemented!()
        // let (tx, rx) = oneshot::channel();
        // self.tx
        //     .send(ActorMessage::SetTag { tag, value, tx })
        //     .await?;
        // Ok(rx.await??)
    }

    async fn create_tag(&self, hash: HashAndFormat) -> OuterResult<Tag> {
        unimplemented!()
        // let (tx, rx) = oneshot::channel();
        // self.tx.send(ActorMessage::CreateTag { hash, tx }).await?;
        // Ok(rx.await??)
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

    // fn temp_file_name(&self) -> PathBuf {
    //     self.path_options.temp_file_name()
    // }

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

/// The actor for the redb store.
///
/// It is split into the database and the rest of the state to allow for split
/// borrows in the message handlers.
struct Actor {
    blobs: HashMap<Hash, EntryState>,
    handles: BTreeMap<Hash, BaoFileHandleWeak>,
    protected: BTreeSet<Hash>,
    msgs_rx: async_channel::Receiver<ActorMessage>,
    create_options: Arc<BaoFileConfig>,
}

/// Error type for message handler functions of the redb actor.
///
/// What can go wrong are various things with redb, as well as io errors related
/// to files other than redb.
#[derive(Debug, thiserror::Error)]
pub(crate) enum ActorError {
    #[error("io error: {0}")]
    Io(#[from] io::Error),
    #[error("inconsistent database state: {0}")]
    Inconsistent(String),
}

impl From<ActorError> for io::Error {
    fn from(e: ActorError) -> Self {
        match e {
            ActorError::Io(e) => e,
            e => io::Error::new(io::ErrorKind::Other, e),
        }
    }
}

/// Result type for handler functions of the store actor.
///
/// See [`ActorError`] for what can go wrong.
pub(crate) type ActorResult<T> = std::result::Result<T, ActorError>;

/// Error type for calling the actor from the store.
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

/// Result type for calling the actor from the store.
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
        bucket: Bucket,
        root: PathBuf,
    ) -> ActorResult<(Self, async_channel::Sender<ActorMessage>)> {
        let (tx, rx) = async_channel::bounded(1024);
        let create_options = BaoFileConfig::new(Arc::new(root.clone()), bucket);
        Ok((
            Self {
                blobs: HashMap::default(),
                handles: BTreeMap::new(),
                protected: BTreeSet::new(),
                msgs_rx: rx,
                create_options: Arc::new(create_options),
            },
            tx,
        ))
    }

    async fn run(mut self) -> ActorResult<()> {
        let mut msgs = PeekableFlumeReceiver::new(self.msgs_rx.clone());
        while let Some(msg) = msgs.recv().await {
            if let ActorMessage::Shutdown { tx } = msg {
                // Make sure the database is dropped before we send the reply.
                // @TODO: What do we need to drop here?
                drop(self);
                if let Some(tx) = tx {
                    tx.send(()).ok();
                }
                break;
            }
            match msg.category() {
                MessageCategory::TopLevel => {
                    self.handle_toplevel(msg)?;
                }
                MessageCategory::ReadOnly => {
                    if let Err(msg) = self.handle_readonly(msg)? {
                        warn!("error handling message in store actor: {msg:?}")
                    };
                }
                MessageCategory::ReadWrite => {
                    if let Err(msg) = self.handle_readwrite(msg)? {
                        warn!("error handling message in store actor: {msg:?}")
                    };
                }
            }
        }
        tracing::debug!("redb actor done");
        Ok(())
    }

    fn entry_status(&mut self, hash: Hash) -> ActorResult<EntryStatus> {
        let status = match self.blobs.get(&hash) {
            Some(entry) => {
                if entry.complete {
                    EntryStatus::Complete
                } else {
                    EntryStatus::Partial
                }
            }
            None => EntryStatus::NotFound,
        };
        Ok(status)
    }

    fn get(&mut self, hash: Hash) -> ActorResult<Option<BaoFileHandle>> {
        if let Some(handle) = self.handles.get(&hash).and_then(|weak| weak.upgrade()) {
            return Ok(Some(handle));
        }

        let Some(entry) = self.blobs.get(&hash) else {
            return Ok(None);
        };

        // todo: if complete, load inline data and/or outboard into memory if needed,
        // and return a complete entry.
        let config = self.create_options.clone();
        let handle = if entry.complete {
            let data = load_data(
                self.create_options.bucket(),
                entry.data_path.clone(),
                entry.size,
            )?;
            let outboard = load_outboard(&self.create_options.paths(&hash).outboard, data.1)?;
            BaoFileHandle::new_complete(config, hash, data, outboard)
        } else {
            BaoFileHandle::incomplete_file(config, hash, entry.data_path.clone(), entry.size)?
        };
        self.handles.insert(hash, handle.downgrade());
        Ok(Some(handle))
    }

    fn get_or_create(&mut self, hash: Hash) -> ActorResult<BaoFileHandle> {
        self.protected.insert(hash);
        if let Some(handle) = self.handles.get(&hash).and_then(|x| x.upgrade()) {
            return Ok(handle);
        }
        let entry = self.blobs.get(&hash);
        let handle = if let Some(entry) = entry {
            let config = self.create_options.clone();
            if entry.complete {
                let data = load_data(
                    self.create_options.bucket(),
                    entry.data_path.clone(),
                    entry.size,
                )?;
                let outboard = load_outboard(&self.create_options.paths(&hash).outboard, data.1)?;
                BaoFileHandle::new_complete(config, hash, data, outboard)
            } else {
                BaoFileHandle::incomplete_file(config, hash, entry.data_path.clone(), entry.size)?
            }
        } else {
            todo!()
        };

        self.handles.insert(hash, handle.downgrade());
        Ok(handle)
    }

    /// Read the entire blobs table. Callers can then sift through the results to find what they need
    fn complete_blobs(&self) -> Vec<io::Result<Hash>> {
        self.blobs
            .iter()
            .filter_map(|(hash, entry)| {
                if entry.complete {
                    Some(Ok(hash.clone()))
                } else {
                    None
                }
            })
            .collect()
    }

    fn incomplete_blobs(&self) -> Vec<io::Result<Hash>> {
        self.blobs
            .iter()
            .filter_map(|(hash, entry)| {
                if !entry.complete {
                    Some(Ok(hash.clone()))
                } else {
                    None
                }
            })
            .collect()
    }

    fn on_complete(&mut self, entry: BaoFileHandle) -> ActorResult<()> {
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
        self.blobs
            .entry(hash)
            .and_modify(|entry| entry.complete = true);
        Ok(())
    }

    fn handle_toplevel(&mut self, msg: ActorMessage) -> ActorResult<()> {
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
        msg: ActorMessage,
    ) -> ActorResult<std::result::Result<(), ActorMessage>> {
        match msg {
            ActorMessage::Get { hash, tx } => {
                let res = self.get(hash);
                tx.send(res).ok();
            }
            ActorMessage::GetOrCreate { hash, tx } => {
                let res = self.get_or_create(hash);
                tx.send(res).ok();
            }
            ActorMessage::EntryStatus { hash, tx } => {
                let res = self.entry_status(hash);
                tx.send(res).ok();
            }
            ActorMessage::CompleteBlobs { tx } => {
                let res = self.complete_blobs();
                tx.send(res).ok();
            }
            ActorMessage::IncompleteBlobs { tx } => {
                let res = self.incomplete_blobs();
                tx.send(res).ok();
            }
            x => return Ok(Err(x)),
        }
        Ok(Ok(()))
    }

    fn handle_readwrite(
        &mut self,
        msg: ActorMessage,
    ) -> ActorResult<std::result::Result<(), ActorMessage>> {
        match msg {
            ActorMessage::OnComplete { handle } => {
                let res = self.on_complete(handle);
                res.ok();
            }
            msg => {
                // try to handle it as readonly
                if let Err(msg) = self.handle_readonly(msg)? {
                    return Ok(Err(msg));
                }
            }
        }
        Ok(Ok(()))
    }
}

fn load_data(bucket: Bucket, path: String, size: u64) -> ActorResult<(S3File, u64)> {
    Ok((S3File::new(bucket, path, size), size))
}

fn load_outboard(outboard_path: &PathBuf, size: u64) -> ActorResult<(File, u64)> {
    let outboard_size = BaoTree::new(size, IROH_BLOCK_SIZE).outboard_size();
    let Ok(file) = std::fs::File::open(&outboard_path) else {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            format!(
                "file not found: {} size={}",
                outboard_path.display(),
                outboard_size
            ),
        )
        .into());
    };
    Ok((file, outboard_size))
}
