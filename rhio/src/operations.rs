// SPDX-License-Identifier: AGPL-3.0-or-later

use std::time::SystemTime;

use anyhow::{Context, Result};
use p2panda_core::{
    validate_backlink, validate_operation, Body, Extension, Header, Operation, PrivateKey,
    PublicKey,
};
use p2panda_net::network::{InEvent, OutEvent};
use p2panda_store::{LogId, LogStore, MemoryStore, OperationStore};
use tokio::sync::{broadcast, mpsc, oneshot};
use tracing::{error, info};

use crate::extensions::RhioExtensions;

#[derive(Debug)]
pub enum ToOperationActor {
    Send {
        text: String,
        reply: oneshot::Sender<Result<()>>,
    },
    Shutdown,
}

pub struct OperationsActor {
    private_key: PrivateKey,
    store: MemoryStore<RhioExtensions>,
    gossip_tx: mpsc::Sender<InEvent>,
    gossip_rx: broadcast::Receiver<OutEvent>,
    inbox: mpsc::Receiver<ToOperationActor>,
    ready_tx: mpsc::Sender<()>,
}

impl OperationsActor {
    pub fn new(
        private_key: PrivateKey,
        store: MemoryStore<RhioExtensions>,
        gossip_tx: mpsc::Sender<InEvent>,
        gossip_rx: broadcast::Receiver<OutEvent>,
        inbox: mpsc::Receiver<ToOperationActor>,
        ready_tx: mpsc::Sender<()>,
    ) -> Self {
        Self {
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

    async fn on_actor_message(&mut self, msg: ToOperationActor) -> Result<bool> {
        match msg {
            ToOperationActor::Send { text, reply } => {
                let result = self.send_message(text).await;
                reply.send(result).ok();
            }
            ToOperationActor::Shutdown => {
                return Ok(false);
            }
        }

        Ok(true)
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
        match ciborium::from_reader::<(Body, Header<RhioExtensions>), _>(&bytes[..]) {
            Ok((body, header)) => {
                let operation = Operation {
                    hash: header.hash(),
                    header,
                    body: Some(body),
                };

                match validate_operation(&operation) {
                    Ok(_) => (),
                    Err(err) => {
                        error!("invalid operation received: {}", err);
                        return;
                    }
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

                info!(
                    "Received operation: {} {} {} {}",
                    operation.header.public_key,
                    operation.header.seq_num,
                    operation.header.timestamp,
                    operation.hash
                );

                self.store
                    .insert_operation(operation)
                    .expect("no errors from memory store");
            }
            Err(err) => {
                error!("invalid message from {delivered_from}: {err}");
            }
        }
    }

    async fn send_message(&mut self, text: String) -> Result<()> {
        let mut body_bytes: Vec<u8> = Vec::new();
        ciborium::ser::into_writer(&text, &mut body_bytes)?;

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

        let body = Body::new(&body_bytes);
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

        self.store.insert_operation(operation)?;

        let mut bytes = Vec::new();
        ciborium::ser::into_writer(&(body, header), &mut bytes)?;

        self.gossip_tx.send(InEvent::Message { bytes }).await?;

        Ok(())
    }
}
