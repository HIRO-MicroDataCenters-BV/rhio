use std::path::PathBuf;
use std::time::SystemTime;

use anyhow::{Context, Result};
use futures_util::StreamExt;
use iroh_blobs::get::db::DownloadProgress;
use iroh_blobs::provider::AddProgress;
use p2panda_blobs::{Blobs, MemoryStore as BlobMemoryStore};
use p2panda_core::operation::{validate_backlink, validate_operation, Body, Header, Operation};
use p2panda_core::{Extension, Hash, PrivateKey, PublicKey};
use p2panda_net::network::{InEvent, OutEvent};
use p2panda_store::{LogId, LogStore, MemoryStore as LogsMemoryStore, OperationStore};
use tokio::sync::{broadcast, mpsc, oneshot};
use tracing::{debug, error, info};

use crate::extensions::RhioExtensions;
use crate::message::{GossipOperation, Message};

#[derive(Debug)]
pub enum ToRhioActor {
    ImportFile {
        path: PathBuf,
        reply: oneshot::Sender<Result<()>>,
    },
    Shutdown,
}

pub struct RhioActor {
    blobs: Blobs<BlobMemoryStore>,
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
        private_key: PrivateKey,
        store: LogsMemoryStore<RhioExtensions>,
        gossip_tx: mpsc::Sender<InEvent>,
        gossip_rx: broadcast::Receiver<OutEvent>,
        inbox: mpsc::Receiver<ToRhioActor>,
        ready_tx: mpsc::Sender<()>,
    ) -> Self {
        Self {
            blobs,
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
            ToRhioActor::ImportFile { path, reply } => {
                let result = self.on_import_file(path).await;
                reply.send(result).ok();
            }
            ToRhioActor::Shutdown => {
                return Ok(false);
            }
        }

        Ok(true)
    }

    async fn on_import_file(&mut self, path: PathBuf) -> Result<()> {
        let mut stream = self.blobs.import_blob(path.clone()).await;
        while let Some(event) = stream.next().await {
            match event.0 {
                AddProgress::AllDone { hash, .. } => {
                    info!("imported file {} with hash {hash}", path.display());
                    let hash = Hash::from_bytes(*hash.as_bytes());
                    self.send_message(Message::AnnounceBlob(hash)).await?;
                }
                _ => (),
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
        let Ok(message) = ciborium::from_reader::<Message, _>(
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
            .insert_operation(operation)
            .expect("no errors from memory store");

        // Handle messages
        match message {
            Message::AnnounceBlob(hash) => {
                if let Err(err) = self.download_blob(hash).await {
                    error!("failed handling announced blob for {hash}: {err}");
                }
            }
        }
    }

    async fn download_blob(&mut self, hash: Hash) -> Result<()> {
        let mut stream = self.blobs.download_blob(hash).await;
        while let Some(event) = stream.next().await {
            match event.0 {
                DownloadProgress::AllDone(_) => {
                    info!("downloaded blob {hash}");
                }
                _ => (),
            }
        }
        Ok(())
    }

    async fn send_message(&mut self, message: Message) -> Result<()> {
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
