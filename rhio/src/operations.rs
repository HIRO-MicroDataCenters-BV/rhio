// SPDX-License-Identifier: AGPL-3.0-or-later

use std::time::SystemTime;

use anyhow::{Context, Result};
use p2panda_core::{Body, Header, Operation, PrivateKey};
use p2panda_net::network::{InEvent, OutEvent};
use p2panda_store::{LogStore, MemoryStore, OperationStore};
use tokio::sync::{broadcast, mpsc};

use crate::extensions::Extensions;

#[derive(Debug)]
pub enum ToOperationActor {
    Send { text: String },
    Shutdown,
}

pub struct OperationsActor {
    private_key: PrivateKey,
    store: MemoryStore<Extensions>,
    gossip_tx: mpsc::Sender<InEvent>,
    gossip_rx: broadcast::Receiver<OutEvent>,
    inbox: mpsc::Receiver<ToOperationActor>,
    ready_tx: mpsc::Sender<()>,
}

impl OperationsActor {
    pub fn new(
        private_key: PrivateKey,
        store: MemoryStore<Extensions>,
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
                    let msg = msg;
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

                }
            }
        }
    }

    async fn on_actor_message(&mut self, msg: ToOperationActor) -> Result<bool> {
        match msg {
            ToOperationActor::Send { text } => {
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
                    extensions: Some(Extensions::default()),
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
            } => match ciborium::from_reader::<(Body, Header<Extensions>), _>(&bytes[..]) {
                Ok((body, header)) => {
                    if header.verify() {
                        println!("{} {} {}", header.seq_num, header.timestamp, header.hash());
                    } else {
                        eprintln!("Invalid operation header received")
                    };
                }
                Err(err) => {
                    eprintln!("invalid message from {delivered_from}: {err}");
                }
            },
        }
    }
}
