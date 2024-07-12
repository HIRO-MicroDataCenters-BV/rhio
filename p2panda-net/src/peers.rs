// SPDX-License-Identifier: AGPL-3.0-or-later

use std::collections::HashMap;

use anyhow::{Context, Result};
use iroh_net::util::SharedAbortingJoinHandle;
use iroh_net::{dns::node_info::NodeInfo, endpoint};
use iroh_net::{Endpoint, NodeId};
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, error, error_span, trace, warn, Instrument};

pub struct PeerState {}

enum ToPeersActor {
    AddPeer { node_info: NodeInfo },
    Shutdown { reply: oneshot::Sender<()> },
}

struct PeersActor {
    inbox: mpsc::Receiver<ToPeersActor>,
    known_peers: HashMap<NodeId, NodeInfo>,
    endpoint: Endpoint,
}

impl PeersActor {
    pub fn new(inbox: mpsc::Receiver<ToPeersActor>, endpoint: Endpoint) -> Self {
        Self {
            inbox,
            known_peers: HashMap::new(),
            endpoint,
        }
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
                self.add_peer(node_info);
            }
            ToPeersActor::Shutdown { .. } => {
                unreachable!("handled in run_inner");
            }
        }
    }

    fn add_peer(&mut self, node_info: NodeInfo) {
        let node_id = node_info.node_id;

        // Make sure the endpoint also knows about this address
        match self.endpoint.add_node_addr(node_info.clone().into()) {
            Ok(_) => {
                if self.known_peers.insert(node_id, node_info).is_none() {
                    debug!("added new peer to handler {}", node_id);
                }
            }
            Err(err) => {
                debug!(
                    "tried to add invalid node {} to known peers list: {err}",
                    node_id
                );
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
    pub fn new(endpoint: Endpoint) -> Self {
        let (actor_tx, actor_rx) = mpsc::channel(64);
        let actor = PeersActor::new(actor_rx, endpoint);

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
