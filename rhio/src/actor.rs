use std::path::PathBuf;
use std::time::SystemTime;

use anyhow::{Context, Result};
use futures_util::StreamExt;
use p2panda_blobs::{Blobs, DownloadBlobEvent, ImportBlobEvent, MemoryStore as BlobMemoryStore};
use p2panda_core::operation::{validate_backlink, validate_operation, Body, Header, Operation};
use p2panda_core::{Extension, Hash, PrivateKey, PublicKey};
use p2panda_net::network::{InEvent, OutEvent};
use p2panda_store::{LogId, LogStore, MemoryStore as LogsMemoryStore, OperationStore};
use tokio::sync::{broadcast, mpsc, oneshot};
use tracing::{debug, error, info};

use crate::aggregate::{FileSystem, FileSystemAction};
use crate::events::{Event, GossipOperation};
use crate::extensions::RhioExtensions;

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
                        self.send_message(Event::Create(relative_path_str.to_string(), hash))
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
                self.on_message(bytes, delivered_from).await;
            }
        }
    }

    async fn on_message(&mut self, bytes: Vec<u8>, delivered_from: PublicKey) {
        // Validate operation
        let Ok(event) = ciborium::from_reader::<GossipOperation, _>(&bytes[..]) else {
            error!("invalid operation from {delivered_from}");
            return;
        };

        let operation = Operation {
            hash: event.header.hash(),
            header: event.header,
            body: Some(event.body),
        };

        if let Err(err) = validate_operation(&operation) {
            error!("invalid operation received: {err}");
            return;
        }

        let log_id: LogId = RhioExtensions::extract(&operation);
        if let Some(latest_operation) = self
            .store
            .latest_operation(operation.header.public_key, log_id)
            .expect("memory store does not error")
        {
            if validate_backlink(&latest_operation.header, &operation.header).is_err() {
                error!("invalid backlink");
                return;
            };
        }

        // Validate message
        let Ok(message) = ciborium::from_reader::<Event, _>(
            &operation
                .body
                .as_ref()
                .expect("body should be given")
                .to_bytes()[..],
        ) else {
            error!("invalid message from {delivered_from}");
            return;
        };

        debug!(
            "Received operation: {} {} {} {} {:?}",
            operation.header.public_key,
            operation.header.seq_num,
            operation.header.timestamp,
            operation.hash,
            message,
        );

        self.store
            .insert_operation(operation.clone())
            .expect("no errors from memory store");

        let actions = self.fs.process(message, operation.header.timestamp);

        for action in actions {
            match action {
                FileSystemAction::DownloadAndExport { hash, path } => {
                    if self.download_blob(hash).await.is_err() {
                        return;
                    }

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
                FileSystemAction::None => (),
            }
        }
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

    async fn send_message(&mut self, message: Event) -> Result<()> {
        // Encode body
        let mut body_bytes: Vec<u8> = Vec::new();
        ciborium::ser::into_writer(&message, &mut body_bytes)?;
        let body = Body::new(&body_bytes);

        // Sign and encode header
        let public_key = self.private_key.public_key();
        let latest_operation = self
            .store
            .latest_operation(public_key, public_key.to_string().into())?;

        let (seq_num, backlink) = match latest_operation {
            Some(operation) => (operation.header.seq_num + 1, Some(operation.hash)),
            None => (0, None),
        };

        let timestamp = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)?
            .as_secs();

        let mut header = Header {
            version: 1,
            public_key,
            signature: None,
            payload_size: body.size(),
            payload_hash: Some(body.hash()),
            timestamp,
            seq_num,
            backlink,
            previous: vec![],
            extensions: Some(RhioExtensions::default()),
        };
        header.sign(&self.private_key);

        let operation = Operation {
            hash: header.hash(),
            header: header.clone(),
            body: Some(body.clone()),
        };

        // Persist operation in our memory store
        self.store.insert_operation(operation)?;

        // Broadcast data in gossip overlay
        let gossip_event = GossipOperation { body, header };
        let mut bytes = Vec::new();
        ciborium::ser::into_writer(&gossip_event, &mut bytes)?;
        self.gossip_tx.send(InEvent::Message { bytes }).await?;

        Ok(())
    }
}
