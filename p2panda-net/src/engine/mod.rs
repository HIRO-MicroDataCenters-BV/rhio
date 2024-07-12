// SPDX-License-Identifier: AGPL-3.0-or-later

mod engine;
mod gossip;

use anyhow::Result;
use iroh_gossip::net::Gossip;
use iroh_net::dns::node_info::NodeInfo;
use iroh_net::util::SharedAbortingJoinHandle;
use iroh_net::Endpoint;
use tokio::sync::{mpsc, oneshot};
use tracing::error;

use engine::{EngineActor, ToEngineActor};
use gossip::GossipActor;

use crate::{NetworkId, TopicId};

pub struct Engine {
    engine_actor_tx: mpsc::Sender<ToEngineActor>,
    #[allow(dead_code)]
    actor_handle: SharedAbortingJoinHandle<()>,
}

impl Engine {
    pub fn new(network_id: NetworkId, endpoint: Endpoint, gossip: Gossip) -> Self {
        let (engine_actor_tx, engine_actor_rx) = mpsc::channel(64);
        let (gossip_actor_tx, gossip_actor_rx) = mpsc::channel(256);

        let engine_actor = EngineActor::new(
            endpoint,
            gossip.clone(),
            gossip_actor_tx,
            engine_actor_rx,
            network_id,
        );
        let gossip_actor = GossipActor::new(gossip_actor_rx, gossip, engine_actor_tx.clone());

        let actor_handle = tokio::task::spawn(async move {
            if let Err(err) = engine_actor.run(gossip_actor).await {
                error!("engine actor failed: {err:?}");
            }
        });

        Self {
            engine_actor_tx,
            actor_handle: actor_handle.into(),
        }
    }

    pub async fn add_peer(&self, node_info: NodeInfo) -> Result<()> {
        self.engine_actor_tx
            .send(ToEngineActor::AddPeer { node_info })
            .await?;
        Ok(())
    }

    pub async fn shutdown(&self) -> Result<()> {
        let (reply, reply_rx) = oneshot::channel();
        self.engine_actor_tx
            .send(ToEngineActor::Shutdown { reply })
            .await?;
        reply_rx.await?;
        Ok(())
    }

    pub async fn subscribe(&self, topic: TopicId) -> Result<()> {
        self.engine_actor_tx
            .send(ToEngineActor::Subscribe { topic })
            .await?;
        Ok(())
    }
}
