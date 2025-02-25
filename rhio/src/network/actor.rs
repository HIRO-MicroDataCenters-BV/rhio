use std::collections::{hash_map, HashMap};

use anyhow::{bail, Result};
use p2panda_net::network::{FromNetwork, ToNetwork};
use p2panda_net::{Network, TopicId};
use tokio::sync::{mpsc, oneshot};
use tracing::{error, trace, warn};

use crate::topic::Query;

pub enum ToPandaActor {
    Broadcast {
        payload: Vec<u8>,
        topic_id: [u8; 32],
        reply: oneshot::Sender<Result<()>>,
    },
    Subscribe {
        query: Query,
        reply: oneshot::Sender<Option<mpsc::Receiver<FromNetwork>>>,
    },
    Shutdown {
        reply: oneshot::Sender<()>,
    },
}

pub struct PandaActor {
    /// p2panda-net network.
    network: Network<Query>,

    /// Map containing senders for all subscribed topics. Messages sent on this channels will be
    /// broadcast to peers interested in the same topic.
    topic_gossip_tx_map: HashMap<[u8; 32], mpsc::Sender<ToNetwork>>,

    /// Actor inbox.
    inbox: mpsc::Receiver<ToPandaActor>,
}

impl PandaActor {
    pub fn new(network: Network<Query>, inbox: mpsc::Receiver<ToPandaActor>) -> Self {
        Self {
            network,
            topic_gossip_tx_map: HashMap::default(),
            inbox,
        }
    }

    pub async fn run(mut self) -> Result<()> {
        // Take oneshot sender from outside API awaited by `shutdown` call and fire it as soon as
        // shutdown completed.
        let shutdown_completed_signal = self.run_inner().await;
        if let Err(err) = self.shutdown().await {
            error!(?err, "error during shutdown");
        }

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
                Some(msg) = self.inbox.recv() => {
                    match msg {
                        ToPandaActor::Shutdown { reply } => {
                            break Ok(reply);
                        }
                        msg => {
                            if let Err(err) = self.on_actor_message(msg).await {
                                warn!(err = ?err, "error processing actor message");
                            }
                        }
                    }
                },
            }
        }
    }

    async fn on_actor_message(&mut self, msg: ToPandaActor) -> Result<bool> {
        match msg {
            ToPandaActor::Subscribe { query, reply } => {
                let result = self.on_subscribe(query).await?;
                reply.send(result).ok();
            }
            ToPandaActor::Broadcast {
                payload,
                topic_id,
                reply,
            } => {
                let result = self.on_broadcast(payload, topic_id).await;
                reply.send(result).ok();
            }
            ToPandaActor::Shutdown { .. } => {
                unreachable!("handled in run_inner");
            }
        }

        Ok(true)
    }

    async fn on_broadcast(&mut self, bytes: Vec<u8>, topic_id: [u8; 32]) -> Result<()> {
        match self.topic_gossip_tx_map.get_mut(&topic_id) {
            Some(tx) => Ok(tx.send(ToNetwork::Message { bytes }).await?),
            None => {
                bail!("attempted to send operation on unknown topic id {topic_id:?}");
            }
        }
    }

    async fn on_subscribe(&mut self, query: Query) -> Result<Option<mpsc::Receiver<FromNetwork>>> {
        let topic_id = query.id();

        if query.is_no_sync() && self.topic_gossip_tx_map.contains_key(&topic_id) {
            return Ok(None);
        }

        trace!(
            topic_id = hex::encode(topic_id),
            %query,
            "subscribe to topic"
        );
        let (tx, rx, _) = self.network.subscribe(query).await?;
        if let hash_map::Entry::Vacant(entry) = self.topic_gossip_tx_map.entry(topic_id) {
            entry.insert(tx);
        }
        Ok(Some(rx))
    }

    async fn shutdown(self) -> Result<()> {
        self.network.shutdown().await?;
        Ok(())
    }
}
