// SPDX-License-Identifier: AGPL-3.0-or-later

use std::collections::HashMap;

use anyhow::{Context, Result};
use iroh_net::dns::node_info::NodeInfo;
use iroh_net::util::SharedAbortingJoinHandle;
use iroh_net::NodeId;
use tokio::sync::{mpsc, oneshot};
use tracing::{error, error_span, trace, Instrument};

pub struct PeerState {}

enum ToPeersActor {
    AddPeer { node_info: NodeInfo },
    Shutdown { reply: oneshot::Sender<()> },
}

struct PeersActor {
    inbox: mpsc::Receiver<ToPeersActor>,
}

impl PeersActor {
    pub fn new(inbox: mpsc::Receiver<ToPeersActor>) -> Self {
        Self { inbox }
    }

    pub async fn run(mut self) -> Result<()> {
        let shutdown_completed_signal = self.run_inner().await;
        if let Err(err) = self.shutdown().await {
            error!(?err, "Error during shutdown");
        }
        drop(self);
        match shutdown_completed_signal {
            Ok(reply_tx) => {
                reply_tx.send(()).ok();
                Ok(())
            }
            Err(err) => Err(err),
        }
    }

    async fn run_inner(&mut self) -> Result<oneshot::Sender<()>> {
        loop {
            tokio::select! {
                biased;
                msg = self.inbox.recv() => {
                    let msg = msg.context("to_actor closed")?;
                    match msg {
                        ToPeersActor::Shutdown { reply } => {
                            break Ok(reply);
                        }
                        msg => {
                            self.on_actor_message(msg).await;
                        }
                    }
                }
            }
        }
    }

    async fn on_actor_message(&mut self, msg: ToPeersActor) {
        match msg {
            ToPeersActor::AddPeer { node_info } => {
                println!("{node_info:?}");
            }
            ToPeersActor::Shutdown { .. } => {
                unreachable!("handled in run_inner");
            }
        }
    }

    async fn shutdown(&mut self) -> Result<()> {
        Ok(())
    }
}

pub struct Peers {
    actor_tx: mpsc::Sender<ToPeersActor>,
    #[allow(dead_code)]
    actor_handle: SharedAbortingJoinHandle<()>,
}

impl Peers {
    pub fn new() -> Self {
        let (actor_tx, actor_rx) = mpsc::channel(64);
        let actor = PeersActor::new(actor_rx);

        let actor_handle = tokio::task::spawn(async move {
            if let Err(err) = actor.run().await {
                error!("peers actor failed: {err:?}");
            }
        });

        Self {
            actor_tx,
            actor_handle: actor_handle.into(),
        }
    }

    pub async fn add_peer(&self, node_info: NodeInfo) -> Result<()> {
        self.actor_tx
            .send(ToPeersActor::AddPeer { node_info })
            .await?;
        Ok(())
    }

    pub async fn shutdown(&self) -> Result<()> {
        let (reply, reply_rx) = oneshot::channel();
        self.actor_tx.send(ToPeersActor::Shutdown { reply }).await?;
        reply_rx.await?;
        Ok(())
    }
}
