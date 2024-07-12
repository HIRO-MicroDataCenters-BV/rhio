// SPDX-License-Identifier: AGPL-3.0-or-later

use std::collections::HashMap;
use std::time::Duration;

use anyhow::{Context, Result};
use iroh_gossip::net::Gossip;
use iroh_gossip::proto::TopicId;
use iroh_net::dns::node_info::NodeInfo;
use iroh_net::key::PublicKey;
use iroh_net::{Endpoint, NodeId};
use rand::seq::IteratorRandom;
use tokio::sync::{mpsc, oneshot};
use tokio::time::interval;
use tracing::{debug, error};

use super::gossip::{GossipActor, ToGossipActor};
use super::message::NetworkMessage;

/// Maximum size of random sample set when choosing peers to join network-wide gossip overlay.
///
/// The larger the number the less likely joining the gossip will fail as we get more chances to
/// establish connections. As soon as we've joined the gossip we will learn about more peers.
const JOIN_NETWORK_PEERS_SAMPLE_LEN: usize = 7;

/// In what frequency do we attempt joining the network-wide gossip overlay over a newly, randomly
/// sampled set of peers.
// NOTE(adz): This is in-place to assure we're not getting stuck with unhealthy connections we've
// found early on but also re-try at a later stage after we've maybe discovered more peers. Is
// there a better way to do this than just trying forever, probably we want to keep some more state
// about the "health" of our connection to the network-wide overlay?
const JOIN_NETWORK_INTERVAL: Duration = Duration::from_secs(9);

/// How often do we announce the list of our subscribed topics.
const ANNOUNCE_TOPICS_INTERVAL: Duration = Duration::from_secs(7);

type Reply<T> = oneshot::Sender<Result<T>>;

pub enum ToEngineActor {
    AddPeer {
        node_info: NodeInfo,
    },
    NeighborDown {
        topic: TopicId,
        peer: PublicKey,
    },
    NeighborUp {
        topic: TopicId,
        peer: PublicKey,
    },
    Subscribe {
        topic: TopicId,
    },
    Received {
        bytes: Vec<u8>,
        delivered_from: PublicKey,
        topic: TopicId,
    },
    Shutdown {
        reply: oneshot::Sender<()>,
    },
}

pub struct EngineActor {
    endpoint: Endpoint,
    gossip: Gossip,
    gossip_actor_tx: mpsc::Sender<ToGossipActor>,
    inbox: mpsc::Receiver<ToEngineActor>,

    /// Address book of known peers.
    known_peers: HashMap<NodeId, NodeInfo>,

    /// Identifier for the whole network. This serves as the topic for the network-wide gossip
    /// overlay.
    network_id: TopicId,

    /// List of inactive subscriptions we've announced to be interested in. We will use the
    /// network-wide gossip overlay to announce our topic interests and hopefully find peers we can
    /// sync with over these topics.
    pending_subscriptions: Vec<TopicId>,

    /// Currently active subscriptions.
    subscriptions: Vec<TopicId>,
}

impl EngineActor {
    pub fn new(
        endpoint: Endpoint,
        gossip: Gossip,
        gossip_actor_tx: mpsc::Sender<ToGossipActor>,
        inbox: mpsc::Receiver<ToEngineActor>,
        network_id: TopicId,
    ) -> Self {
        Self {
            endpoint,
            gossip,
            gossip_actor_tx,
            inbox,
            known_peers: HashMap::new(),
            network_id,
            pending_subscriptions: Vec::new(),
            subscriptions: Vec::new(),
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
        let mut join_network_interval = interval(JOIN_NETWORK_INTERVAL);
        let mut announce_topics_interval = interval(ANNOUNCE_TOPICS_INTERVAL);

        loop {
            tokio::select! {
                biased;
                msg = self.inbox.recv() => {
                    let msg = msg.context("inbox closed")?;
                    match msg {
                        ToEngineActor::Shutdown { reply } => {
                            break Ok(reply);
                        }
                        msg => {
                            if let Err(err) = self.on_actor_message(msg).await {
                                break Err(err);
                            }
                        }
                    }
                },
                _ = announce_topics_interval.tick() => {
                    self.announce_topics().await?;
                },
                _ = join_network_interval.tick() => {
                    self.subscribe_to_network().await?;
                },
            }
        }
    }

    async fn on_actor_message(&mut self, msg: ToEngineActor) -> Result<()> {
        match msg {
            ToEngineActor::AddPeer { node_info } => {
                self.add_peer(node_info);
            }
            ToEngineActor::NeighborUp { topic, peer } => {
                println!("Found gossip partner for {:?} {:?}", topic, peer);
                // @TODO
            }
            ToEngineActor::NeighborDown { topic, peer } => {
                println!("Gossip partner left for {:?} {:?}", topic, peer);
                // @TODO
            }
            ToEngineActor::Subscribe { topic } => {
                self.subscribe(topic);
            }
            ToEngineActor::Received {
                bytes,
                delivered_from,
                topic,
            } => {
                self.on_gossip_message(bytes, delivered_from, topic)?;
            }
            ToEngineActor::Shutdown { .. } => {
                unreachable!("handled in run_inner");
            }
        }

        Ok(())
    }

    /// Adds a new peer to our address book.
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
                // This can fail if we're trying to add ourselves
                debug!(
                    "tried to add invalid node {} to known peers list: {err}",
                    node_id
                );
            }
        }
    }

    /// Join the network-wide gossip overlay by picking a random sample set from the currently
    /// known peers.
    ///
    /// The network-wide overlay is used to learn about which peers are interested in which topics
    /// (we can then subscribe to).
    async fn subscribe_to_network(&mut self) -> Result<()> {
        let peers = self
            .known_peers
            .values()
            .choose_multiple(&mut rand::thread_rng(), JOIN_NETWORK_PEERS_SAMPLE_LEN)
            .iter()
            .map(|peer| peer.node_id)
            .collect();

        self.gossip_actor_tx
            .send(ToGossipActor::Join {
                topic: self.network_id,
                peers,
            })
            .await?;

        Ok(())
    }

    fn subscribe(&mut self, topic: TopicId) {
        if self.subscriptions.contains(&topic) {
            return;
        }

        if self.pending_subscriptions.contains(&topic) {
            return;
        }

        self.pending_subscriptions.push(topic);
    }

    async fn announce_topics(&mut self) -> Result<()> {
        let mut topics = Vec::new();
        topics.extend(&self.pending_subscriptions);
        topics.extend(&self.subscriptions);
        topics.sort_unstable();
        self.broadcast_to_network(NetworkMessage::Announcement(topics))
            .await?;
        Ok(())
    }

    async fn broadcast_to_network(&mut self, message: NetworkMessage) -> Result<()> {
        if !self.has_joined_topic(self.network_id).await? {
            debug!(
                "Has not joined network id {} yet, skip broadcasting",
                self.network_id
            );
            return Ok(());
        }
        debug!("broadcast to network {:?}", message);
        let bytes = message.to_bytes()?;
        self.gossip
            .broadcast_neighbors(self.network_id, bytes.into())
            .await?;
        Ok(())
    }

    async fn has_joined_topic(&mut self, topic: TopicId) -> Result<bool> {
        let (reply, reply_rx) = oneshot::channel();
        self.gossip_actor_tx
            .send(ToGossipActor::HasJoined { topic, reply })
            .await
            .ok();
        reply_rx.await?
    }

    fn on_gossip_message(
        &mut self,
        bytes: Vec<u8>,
        delivered_from: PublicKey,
        topic: TopicId,
    ) -> Result<()> {
        // Message coming from network-wide gossip overlay
        if topic == self.network_id {
            let message = NetworkMessage::from_bytes(&bytes)
                .context("parsing network-wide gossip message")?;
            match message {
                NetworkMessage::Announcement(topics) => {
                    debug!(
                        "Received announcement of peer {} {:?}",
                        delivered_from, topics
                    );
                }
            }
        }

        Ok(())
    }

    async fn shutdown(&mut self) -> Result<()> {
        self.gossip_actor_tx
            .send(ToGossipActor::Shutdown)
            .await
            .ok();
        Ok(())
    }
}
