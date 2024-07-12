// SPDX-License-Identifier: AGPL-3.0-or-later

use std::collections::HashMap;

use anyhow::{Context, Result};
use iroh_gossip::proto::TopicId;
use iroh_net::dns::node_info::NodeInfo;
use iroh_net::key::PublicKey;
use iroh_net::{Endpoint, NodeId};
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, error};

use super::gossip::{GossipActor, ToGossipActor};

pub enum ToEngineActor {
    AddPeer { node_info: NodeInfo },
    Shutdown { reply: oneshot::Sender<()> },
    NeighborUp { topic: TopicId, peer: PublicKey },
    NeighborDown { topic: TopicId, peer: PublicKey },
}

pub struct EngineActor {
    inbox: mpsc::Receiver<ToEngineActor>,
    endpoint: Endpoint,
    gossip_actor_tx: mpsc::Sender<ToGossipActor>,
    known_peers: HashMap<NodeId, NodeInfo>,
}

impl EngineActor {
    pub fn new(
        inbox: mpsc::Receiver<ToEngineActor>,
        gossip_actor_tx: mpsc::Sender<ToGossipActor>,
        endpoint: Endpoint,
    ) -> Self {
        Self {
            inbox,
            endpoint,
            gossip_actor_tx,
            known_peers: HashMap::new(),
        }
    }

    pub async fn run(mut self, mut gossip_actor: GossipActor) -> Result<()> {
        let gossip_handle = tokio::task::spawn(async move {
            if let Err(err) = gossip_actor.run().await {
                error!("gossip recv actor failed: {err:?}");
            }
        });

        let shutdown_completed_signal = self.run_inner().await;
        if let Err(err) = self.shutdown().await {
            error!(?err, "Error during shutdown");
        }
        gossip_handle.await?;
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
                        ToEngineActor::Shutdown { reply } => {
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

    async fn on_actor_message(&mut self, msg: ToEngineActor) {
        match msg {
            ToEngineActor::AddPeer { node_info } => {
                self.add_peer(node_info);
            }
            ToEngineActor::Shutdown { .. } => {
                unreachable!("handled in run_inner");
            }
            ToEngineActor::NeighborUp { topic, peer } => todo!(),
            ToEngineActor::NeighborDown { topic, peer } => todo!(),
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
        self.gossip_actor_tx
            .send(ToGossipActor::Shutdown)
            .await
            .ok();

        Ok(())
    }
}
