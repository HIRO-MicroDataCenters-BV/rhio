use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::pin::Pin;

use anyhow::{Context, Result};
use p2panda_blobs::{Blobs, DownloadBlobEvent, ImportBlobEvent, MemoryStore as BlobMemoryStore};
use p2panda_core::{Hash, PrivateKey};
use p2panda_net::network::{InEvent, OutEvent};
use p2panda_store::MemoryStore as LogsMemoryStore;
use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio_stream::{Stream, StreamExt, StreamMap};
use tracing::{debug, error, info};

use crate::aggregate::{FileSystem, FileSystemAction};
use crate::extensions::RhioExtensions;
use crate::messages::{FileSystemEvent, GossipOperation, Message};
use crate::operations::{create, ingest};
use crate::topic_id::TopicId;
use crate::{BLOB_ANNOUNCE_TOPIC, FILE_SYSTEM_EVENT_TOPIC};

pub enum ToRhioActor<T> {
    SyncFile {
        absolute_path: PathBuf,
        relative_path: PathBuf,
        reply: oneshot::Sender<Result<()>>,
    },
    ImportBlob {
        path: PathBuf,
        reply: oneshot::Sender<Result<()>>,
    },
    PublishEvent {
        topic: TopicId,
        message: Message<T>,
        reply: oneshot::Sender<Result<()>>,
    },
    Subscribe {
        topic: TopicId,
        reply: oneshot::Sender<broadcast::Receiver<Message<T>>>,
    },
    Shutdown,
}

pub struct RhioActor<T> {
    fs: FileSystem,
    blobs: Blobs<BlobMemoryStore>,
    blobs_export_path: PathBuf,
    exported_blobs: HashMap<PathBuf, Hash>,
    private_key: PrivateKey,
    store: LogsMemoryStore<RhioExtensions>,
    gossip_tx: HashMap<TopicId, mpsc::Sender<InEvent>>,
    gossip_rx: StreamMap<TopicId, Pin<Box<dyn Stream<Item = OutEvent> + Send + 'static>>>,
    topic_clients_tx: HashMap<TopicId, broadcast::Sender<Message<T>>>,
    inbox: mpsc::Receiver<ToRhioActor<T>>,
    pending_topics: HashSet<TopicId>,
    ready_tx: mpsc::Sender<()>,
}

impl<T> RhioActor<T>
where
    T: Serialize + DeserializeOwned + Clone + std::fmt::Debug,
{
    pub fn new(
        blobs: Blobs<BlobMemoryStore>,
        blobs_export_path: PathBuf,
        private_key: PrivateKey,
        store: LogsMemoryStore<RhioExtensions>,
        gossip_tx: HashMap<TopicId, mpsc::Sender<InEvent>>,
        gossip_rx: StreamMap<TopicId, Pin<Box<dyn Stream<Item = OutEvent> + Send + 'static>>>,
        inbox: mpsc::Receiver<ToRhioActor<T>>,
        ready_tx: mpsc::Sender<()>,
    ) -> Self {
        let mut pending_topics = HashSet::new();
        for topic in gossip_tx.keys() {
            pending_topics.insert(*topic);
        }

        Self {
            fs: FileSystem::new(),
            blobs,
            blobs_export_path,
            exported_blobs: HashMap::new(),
            private_key,
            store,
            gossip_tx,
            gossip_rx,
            topic_clients_tx: HashMap::new(),
            inbox,
            pending_topics,
            ready_tx,
        }
    }

    pub async fn run(&mut self) -> Result<()> {
        loop {
            tokio::select! {
                Some(msg) = self.inbox.recv() => {
                    if !self
                        .on_actor_message(msg)
                        .await
                        .context("on_actor_message")?
                    {
                        return Ok(());
                    }

                },
                Some((topic_id, msg)) = self.gossip_rx.next() => {
                    self
                        .on_gossip_event(topic_id, msg)
                        .await;
                },
            }
        }
    }

    async fn send_operation(
        &mut self,
        topic: TopicId,
        operation: GossipOperation<T>,
    ) -> Result<()> {
        match self.gossip_tx.get_mut(&topic) {
            Some(tx) => {
                tx.send(InEvent::Message {
                    bytes: operation.to_bytes(),
                })
                .await
            }
            None => {
                return Err(anyhow::anyhow!(
                    "Attempted to send operation on unknown topic {topic:?}"
                ))
            }
        }?;
        Ok(())
    }

    async fn on_actor_message(&mut self, msg: ToRhioActor<T>) -> Result<bool> {
        match msg {
            ToRhioActor::SyncFile {
                absolute_path,
                relative_path,
                reply,
            } => {
                let result = self.on_sync_file(absolute_path, relative_path).await;
                reply.send(result).ok();
            }
            ToRhioActor::ImportBlob { path, reply } => {
                let result = self.on_import_blob(path).await;
                reply.send(result).ok();
            }
            ToRhioActor::PublishEvent {
                topic,
                message,
                reply,
            } => {
                let result = self.on_publish_event(topic, message).await;
                reply.send(result).ok();
            }
            ToRhioActor::Shutdown => {
                return Ok(false);
            }
            ToRhioActor::Subscribe { topic, reply } => {
                if let Some(tx) = self.topic_clients_tx.get(&topic) {
                    reply.send(tx.subscribe()).ok();
                } else {
                    let (tx, rx) = broadcast::channel(128);
                    self.topic_clients_tx.insert(topic, tx);
                    let _ = reply.send(rx).ok();
                };
            }
        }

        Ok(true)
    }

    async fn on_publish_event(&mut self, topic: TopicId, message: Message<T>) -> Result<()> {
        let operation = self.create_operation(topic, message).await?;
        self.send_operation(topic, operation).await
    }

    async fn create_operation(
        &mut self,
        topic: TopicId,
        message: Message<T>,
    ) -> Result<GossipOperation<T>> {
        // The log id is {PUBLIC_KEY}/{SUFFIX} string.
        let log_id = format!(
            "{}/{}",
            self.private_key.public_key().to_hex(),
            topic.to_string()
        )
        .into();

        // Create an operation for this event.
        let operation = create(&mut self.store, &self.private_key, &log_id, &message)?;

        // Broadcast data in gossip overlay
        let operation = GossipOperation {
            header: operation.header,
            message,
        };

        Ok(operation)
    }

    async fn on_import_blob(&mut self, path: PathBuf) -> Result<()> {
        let mut stream = self.blobs.import_blob(path.clone()).await;
        while let Some(event) = stream.next().await {
            match event {
                ImportBlobEvent::Abort(err) => {
                    error!("failed importing file: {err}");
                }
                ImportBlobEvent::Done(hash) => {
                    info!("imported file {path:?} with hash {hash}");
                    self.send_blob_announcement_event(hash).await?;
                }
            }
        }
        Ok(())
    }

    async fn on_sync_file(&mut self, absolute_path: PathBuf, relative_path: PathBuf) -> Result<()> {
        // Don't import blobs which just got exported to the filesystem again.
        if let Some(exported_blob_hash) = self.exported_blobs.remove(&relative_path) {
            if self.fs.file_announced(exported_blob_hash, &relative_path) {
                return Ok(());
            }
        };

        let mut stream = self.blobs.import_blob(absolute_path.clone()).await;
        while let Some(event) = stream.next().await {
            match event {
                ImportBlobEvent::Abort(err) => {
                    error!("failed importing file: {err}");
                }
                ImportBlobEvent::Done(hash) => {
                    info!("imported file {absolute_path:?} with hash {hash}");
                    if self.fs.file_announced(hash, &relative_path) {
                        return Ok(());
                    }
                    self.send_fs_event(relative_path.clone(), hash).await?;
                }
            }
        }
        Ok(())
    }

    async fn on_gossip_event(&mut self, topic: TopicId, event: OutEvent) {
        match event {
            OutEvent::Ready => {
                self.pending_topics.remove(&topic);
                if self.pending_topics.is_empty() {
                    self.ready_tx.send(()).await.ok();
                }
            }
            OutEvent::Message {
                bytes,
                delivered_from,
            } => {
                // Ingest the operation, this performs all expected validation.
                let operation = match GossipOperation::from_bytes(&bytes) {
                    Ok(operation) => operation,
                    Err(err) => {
                        error!("Failed to decode gossip operation: {err}");
                        return;
                    }
                };

                match ingest(
                    &mut self.store,
                    operation.header.clone(),
                    Some(operation.body()),
                ) {
                    Ok(result) => result,
                    Err(err) => {
                        error!("Failed to ingest operation from {delivered_from}: {err}");
                        return;
                    }
                };

                debug!(
                    "Received operation: {} {} {} {}",
                    operation.header.public_key,
                    operation.header.seq_num,
                    operation.header.timestamp,
                    operation.header.hash(),
                );

                match &operation.message {
                    Message::FileSystem(event) => {
                        self.on_fs_event(event.clone(), operation.header.timestamp)
                            .await
                    }
                    Message::BlobAnnouncement(hash) => self.on_blob_announcement_event(*hash).await,
                    Message::Application(message) => {
                    }
                }

                let tx = self.topic_clients_tx.get(&topic).expect("topic is known");
                let _ = tx.send(operation.message.clone());
            }
        }
    }

    async fn on_blob_announcement_event(&mut self, hash: Hash) {
        if self.download_blob(hash).await.is_err() {
            return;
        }
    }

    async fn send_blob_announcement_event(&mut self, hash: Hash) -> Result<()> {
        let topic = TopicId::from_str(BLOB_ANNOUNCE_TOPIC);
        let message = Message::BlobAnnouncement(hash);
        let operation = self.create_operation(topic, message).await?;
        self.send_operation(topic, operation).await
    }

    async fn on_fs_event(&mut self, event: FileSystemEvent, timestamp: u64) {
        // Process the event and run any generated actions.
        let actions = self.fs.process(event, timestamp);
        for action in actions {
            match action {
                FileSystemAction::DownloadAndExport { hash, path } => {
                    if self.download_blob(hash).await.is_err() {
                        return;
                    }
                    self.export_blob(hash, path).await;
                }
                FileSystemAction::Export { hash, path } => {
                    self.export_blob(hash, path).await;
                }
            }
        }
    }

    async fn send_fs_event(&mut self, path: PathBuf, hash: Hash) -> Result<()> {
        let topic = TopicId::from_str(FILE_SYSTEM_EVENT_TOPIC);
        let fs_event = FileSystemEvent::Create(path, hash);
        let message = Message::FileSystem(fs_event.clone());
        let operation = self.create_operation(topic, message).await?;
        self.send_operation(topic, operation).await
    }

    async fn download_blob(&mut self, hash: Hash) -> Result<()> {
        let mut stream = self.blobs.download_blob(hash).await;
        while let Some(event) = stream.next().await {
            match event {
                DownloadBlobEvent::Abort(err) => {
                    error!("failed downloading file: {err}");
                }
                DownloadBlobEvent::Done => {
                    info!("downloaded blob {hash}");
                }
            }
        }
        Ok(())
    }

    async fn export_blob(&mut self, hash: Hash, path: PathBuf) {
        let path_str = path.to_str().expect("is a valid unicode str");

        match self
            .blobs
            .export_blob(hash, &self.blobs_export_path, path_str)
            .await
        {
            Ok(_) => {
                info!("exported blob to filesystem {path_str} {hash}");
                self.exported_blobs.insert(path, hash);
            }
            Err(err) => error!("failed to export blob to filesystem {err}"),
        };
    }
}
