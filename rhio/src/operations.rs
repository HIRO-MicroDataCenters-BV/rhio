// SPDX-License-Identifier: AGPL-3.0-or-later

use std::time::SystemTime;

use anyhow::{Context, Result};
use p2panda_core::{Body, Header, Operation, PrivateKey};
use p2panda_net::network::InEvent;
use p2panda_store::{LogStore, MemoryStore, OperationStore};
use tokio::sync::mpsc;

use crate::{extensions::Extensions, node::Message};

#[derive(Debug)]
pub enum ToOperationActor {
    Send {
        text: String,
    },
    Receive {
        header: Header<Extensions>,
        body: Body,
    },
    Shutdown,
}

pub struct OperationsActor {
    private_key: PrivateKey,
    store: MemoryStore<Extensions>,
    gossip_tx: mpsc::Sender<InEvent>,
    inbox: mpsc::Receiver<ToOperationActor>,
}

impl OperationsActor {
    pub fn new(
        private_key: PrivateKey,
        store: MemoryStore<Extensions>,
        gossip_tx: mpsc::Sender<InEvent>,
        inbox: mpsc::Receiver<ToOperationActor>,
    ) -> Self {
        Self {
            private_key,
            store,
            gossip_tx,
            inbox,
        }
    }

    pub async fn run(&mut self) -> Result<()> {
        while let Some(msg) = self.inbox.recv().await {
            let msg = msg;
            if !self
                .on_actor_message(msg)
                .await
                .context("on_actor_message")?
            {
                break;
            }
        }

        Ok(())
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
                    body: Some(body),
                };

                self.store.insert_operation(operation)?;

                let message = Message {
                    header,
                    text: text.to_string(),
                };

                let mut bytes = Vec::new();
                ciborium::ser::into_writer(&message, &mut bytes)?;

                self.gossip_tx.send(InEvent::Message { bytes }).await?;
            }
            ToOperationActor::Receive { header, body } => todo!(),
            ToOperationActor::Shutdown => {
                return Ok(false);
            }
        }

        Ok(true)
    }
}
