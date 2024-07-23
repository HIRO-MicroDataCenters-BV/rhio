use std::path::PathBuf;

use anyhow::{Context, Result};
use futures_util::StreamExt;
use p2panda_blobs::{Blobs, DownloadBlobEvent, ImportBlobEvent, MemoryStore as BlobMemoryStore};
use p2panda_core::{Hash, PrivateKey, PublicKey};
use p2panda_net::network::{InEvent, OutEvent};
use p2panda_store::MemoryStore as LogsMemoryStore;
use tokio::sync::{broadcast, mpsc, oneshot};
use tracing::{debug, error, info};

use crate::aggregate::{FileSystem, FileSystemAction};
use crate::extensions::RhioExtensions;
use crate::messages::FileSystemEvent;
use crate::operations::{create, decode_header_and_body, encode_header_and_body, ingest};

#[derive(Debug)]
pub enum ToRhioActor {
    ImportFile {
        absolute_path: PathBuf,
        relative_path: PathBuf,
        reply: oneshot::Sender<Result<()>>,
    },
    Shutdown,
}

pub struct RhioActor {
    fs: FileSystem,
    blobs: Blobs<BlobMemoryStore>,
    blobs_export_path: PathBuf,
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
            ToRhioActor::ImportFile {
                absolute_path,
                relative_path,
                reply,
            } => {
                let result = self.on_import_file(absolute_path, relative_path).await;
                reply.send(result).ok();
            }
            ToRhioActor::Shutdown => {
                return Ok(false);
            }
        }

        Ok(true)
    }

    async fn on_import_file(
        &mut self,
        absolute_path: PathBuf,
        relative_path: PathBuf,
    ) -> Result<()> {
        let relative_path_str = relative_path.to_str().expect("is a valid unicode str");

        if self.fs.file_exists(&PathBuf::from(relative_path_str)) {
            return Ok(());
        }

        let mut stream = self.blobs.import_blob(absolute_path.clone()).await;
        while let Some(event) = stream.next().await {
            match event {
                ImportBlobEvent::Abort(err) => {
                    error!("failed importing file: {err}");
                }
                ImportBlobEvent::Done(hash) => {
                    info!("imported file {} with hash {hash}", absolute_path.display());
                    let hash = Hash::from_bytes(*hash.as_bytes());

                    if !self
                        .fs
                        .file_announced(hash, &PathBuf::from(relative_path_str))
                    {
                        self.send_fs_event(FileSystemEvent::Create(
                            relative_path_str.to_string(),
                            hash,
                        ))
                        .await?;
                    }
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
                self.on_fs_event(bytes, delivered_from).await;
            }
        }
    }

    async fn on_fs_event(&mut self, bytes: Vec<u8>, delivered_from: PublicKey) {
        // Validate operation
        let Ok((body, header)) = decode_header_and_body(&bytes) else {
            error!("invalid operation from {delivered_from}");
            return;
        };

        // Decode and validate the operation body (currently we only expect FileSystemEvents in
        // the body).
        let fs_event = body
            .as_ref()
            .map(|body| FileSystemEvent::from_bytes(&body.to_bytes()).expect("valid body bytes"));

        // Ingest the operation, this performs all expected validation.
        match ingest(&mut self.store, header.clone(), body) {
            Ok(result) => result,
            Err(err) => {
                error!("Failed to ingest operation from {delivered_from}: {err}");
                return;
            }
        };

        debug!(
            "Received operation: {} {} {} {} {:?}",
            header.public_key,
            header.seq_num,
            header.timestamp,
            header.hash(),
            fs_event,
        );

        // Process the event and run any generated actions.
        if let Some(fs_event) = fs_event {
            let actions = self.fs.process(fs_event, header.timestamp);
            for action in actions {
                self.handle_fs_action(action).await;
            }
        }
    }

    async fn send_fs_event(&mut self, fs_event: FileSystemEvent) -> Result<()> {
        // Create an operation for this event.
        let operation = create(&mut self.store, &self.private_key, &fs_event)?;

        // Send the event to the FileSystem aggregator, we don't expect any actions
        // to come back.
        let _ = self.fs.process(fs_event, operation.header.timestamp);

        // Broadcast data in gossip overlay
        let bytes = encode_header_and_body(operation.header, operation.body)?;
        self.gossip_tx.send(InEvent::Message { bytes }).await?;

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

    async fn handle_fs_action(&mut self, action: FileSystemAction) {
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

    async fn export_blob(&mut self, hash: Hash, path: PathBuf) {
        let path_str = path.to_str().expect("is a valid unicode str");

        match self
            .blobs
            .export_blob(hash, &self.blobs_export_path, path_str)
            .await
        {
            Ok(_) => info!("exported blob to filesystem {path_str}"),
            Err(err) => error!("failed to export blob to filesystem {err}"),
        };
    }
}
