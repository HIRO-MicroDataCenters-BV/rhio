use std::collections::HashMap;
use std::path::PathBuf;

use anyhow::{Context, Result};
use futures_util::StreamExt;
use p2panda_blobs::{Blobs, DownloadBlobEvent, ImportBlobEvent, MemoryStore as BlobMemoryStore};
use p2panda_core::{Hash, PrivateKey};
use p2panda_net::network::{InEvent, OutEvent};
use p2panda_store::MemoryStore as LogsMemoryStore;
use tokio::sync::{broadcast, mpsc, oneshot};
use tracing::{debug, error, info};

use crate::aggregate::{FileSystem, FileSystemAction};
use crate::extensions::RhioExtensions;
use crate::messages::{FileSystemEvent, GossipOperation, Message};
use crate::operations::{create, ingest};
use crate::{BLOB_ANNOUNCEMENT_LOG_SUFFIX, FILE_SYSTEM_SYNC_LOG_SUFFIX};

#[derive(Debug)]
pub enum ToRhioActor {
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
        log_suffix: String,
        bytes: Vec<u8>,
        reply: oneshot::Sender<Result<()>>,
    },
    Shutdown,
}

pub struct RhioActor {
    fs: FileSystem,
    blobs: Blobs<BlobMemoryStore>,
    blobs_export_path: PathBuf,
    exported_blobs: HashMap<PathBuf, Hash>,
    private_key: PrivateKey,
    store: LogsMemoryStore<RhioExtensions>,
    gossip_tx: mpsc::Sender<InEvent>,
    gossip_rx: broadcast::Receiver<OutEvent>,
    inbox: mpsc::Receiver<ToRhioActor>,
    ready_tx: mpsc::Sender<()>,
}

impl RhioActor {
    pub fn new(
        blobs: Blobs<BlobMemoryStore>,
        blobs_export_path: PathBuf,
        private_key: PrivateKey,
        store: LogsMemoryStore<RhioExtensions>,
        gossip_tx: mpsc::Sender<InEvent>,
        gossip_rx: broadcast::Receiver<OutEvent>,
        inbox: mpsc::Receiver<ToRhioActor>,
        ready_tx: mpsc::Sender<()>,
    ) -> Self {
        Self {
            fs: FileSystem::new(),
            blobs,
            blobs_export_path,
            exported_blobs: HashMap::new(),
            private_key,
            store,
            gossip_tx,
            gossip_rx,
            inbox,
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
                msg = self.gossip_rx.recv() => {
                    let msg = msg?;
                    self
                        .on_gossip_event(msg)
                        .await;
                },
            }
        }
    }

    async fn on_actor_message(&mut self, msg: ToRhioActor) -> Result<bool> {
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
                log_suffix,
                bytes,
                reply,
            } => {
                let result = self.on_publish_event(&log_suffix, bytes).await;
                reply.send(result).ok();
            }
            ToRhioActor::Shutdown => {
                return Ok(false);
            }
        }

        Ok(true)
    }

    async fn on_publish_event(&mut self, log_suffix: &str, bytes: Vec<u8>) -> Result<()> {
        self.publish_operation(log_suffix, Message::Arbitrary(bytes))
            .await
    }

    async fn publish_operation(&mut self, log_suffix: &str, message: Message) -> Result<()> {
        // The log id is {PUBLIC_KEY}/{SUFFIX} string.
        let log_id = format!("{}/{}", self.private_key.public_key().to_hex(), log_suffix).into();

        // Create an operation for this event.
        let operation = create(&mut self.store, &self.private_key, &log_id, &message)?;

        // Broadcast data in gossip overlay
        let bytes = GossipOperation {
            header: operation.header,
            message,
        }
        .to_bytes();

        self.gossip_tx.send(InEvent::Message { bytes }).await?;

        Ok(())
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

    async fn on_gossip_event(&mut self, event: OutEvent) {
        match event {
            OutEvent::Ready => {
                self.ready_tx.send(()).await.ok();
            }
            OutEvent::Message {
                bytes,
                delivered_from,
            } => {
                let operation = match GossipOperation::from_bytes(&bytes) {
                    Ok(operation) => operation,
                    Err(err) => {
                        error!("failed to decode gossip operaiton: {err}");
                        return;
                    }
                };

                // Ingest the operation, this performs all expected validation.
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

                match operation.message {
                    Message::FileSystem(event) => {
                        self.on_fs_event(event, operation.header.timestamp).await
                    }
                    Message::BlobAnnouncement(hash) => self.on_blob_announcement_event(hash).await,
                    Message::Arbitrary(_) => todo!(),
                }
            }
        }
    }

    async fn on_blob_announcement_event(&mut self, hash: Hash) {
        if self.download_blob(hash).await.is_err() {
            return;
        }
    }

    async fn send_blob_announcement_event(&mut self, hash: Hash) -> Result<()> {
        let message = Message::BlobAnnouncement(hash);
        self.publish_operation(BLOB_ANNOUNCEMENT_LOG_SUFFIX, message)
            .await?;
        Ok(())
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
        let fs_event = FileSystemEvent::Create(path, hash);
        let message = Message::FileSystem(fs_event.clone());
        self.publish_operation(FILE_SYSTEM_SYNC_LOG_SUFFIX, message)
            .await?;
        Ok(())
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
