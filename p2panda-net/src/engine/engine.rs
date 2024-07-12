// SPDX-License-Identifier: AGPL-3.0-or-later

use std::collections::HashMap;
use std::time::Duration;

use anyhow::{Context, Result};
use iroh_gossip::net::Gossip;
use iroh_gossip::proto::TopicId;
use iroh_net::dns::node_info::NodeInfo;
use iroh_net::key::PublicKey;
use iroh_net::{Endpoint, NodeId};
use p2panda_core::PublicKey as PandaPublicKey;
use rand::seq::IteratorRandom;
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::time::interval;
use tracing::{debug, error};

use crate::network::{InEvent, OutEvent};

use super::gossip::{GossipActor, ToGossipActor};
use super::message::NetworkMessage;

/// Maximum size of random sample set when choosing peers to join network-wide gossip overlay.
///
/// The larger the number the less likely joining the gossip will fail as we get more chances to
/// establish connections. As soon as we've joined the gossip we will learn about more peers.
const JOIN_PEERS_SAMPLE_LEN: usize = 7;

/// In what frequency do we attempt joining the network-wide gossip overlay over a newly, randomly
/// sampled set of peers.
// NOTE(adz): This is in-place to assure we're not getting stuck with unhealthy connections we've
// found early on but also re-try at a later stage after we've maybe discovered more peers. Is
// there a better way to do this than just trying forever, probably we want to keep some more state
// about the "health" of our connection to the network-wide overlay?
const JOIN_NETWORK_INTERVAL: Duration = Duration::from_secs(9);

/// How often do we announce the list of our subscribed topics.
const ANNOUNCE_TOPICS_INTERVAL: Duration = Duration::from_secs(7);

/// How often do we try to join the topics we're interested in.
const JOIN_TOPICS_INTERVAL: Duration = Duration::from_secs(7);

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
        out_tx: broadcast::Sender<OutEvent>,
        in_rx: mpsc::Receiver<InEvent>,
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

type SubscriptionChannels = broadcast::Sender<OutEvent>;

pub struct EngineActor {
    endpoint: Endpoint,
    gossip: Gossip,
    gossip_actor_tx: mpsc::Sender<ToGossipActor>,
    inbox: mpsc::Receiver<ToEngineActor>,

    known_topics: HashMap<TopicId, Vec<PublicKey>>,

    /// Address book of known peers.
    known_peers: HashMap<NodeId, NodeInfo>,

    /// Identifier for the whole network. This serves as the topic for the network-wide gossip
    /// overlay.
    network_id: TopicId,

    /// List of inactive subscriptions we've announced to be interested in. We will use the
    /// network-wide gossip overlay to announce our topic interests and hopefully find peers we can
    /// sync with over these topics.
    pending_subscriptions: HashMap<TopicId, SubscriptionChannels>,

    /// Currently active subscriptions.
    subscriptions: HashMap<TopicId, SubscriptionChannels>,
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
            known_topics: HashMap::new(),
            known_peers: HashMap::new(),
            network_id,
            pending_subscriptions: HashMap::new(),
            subscriptions: HashMap::new(),
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
        let mut join_topics_interval = interval(JOIN_TOPICS_INTERVAL);

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
                _ = join_topics_interval.tick() => {
                    self.subscribe_to_topics().await?;
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
                self.add_peer(node_info).await?;
            }
            ToEngineActor::NeighborUp { topic, peer } => {
                println!("found peer {} for topic {}", peer, topic);
                // @TODO
            }
            ToEngineActor::NeighborDown { topic, peer } => {
                // @TODO
            }
            ToEngineActor::Subscribe {
                topic,
                out_tx,
                in_rx,
            } => {
                self.pending_subscribe(topic, out_tx, in_rx);
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
    async fn add_peer(&mut self, node_info: NodeInfo) -> Result<()> {
        let node_id = node_info.node_id;

        // Make sure the endpoint also knows about this address
        match self.endpoint.add_node_addr(node_info.clone().into()) {
            Ok(_) => {
                if self.known_peers.insert(node_id, node_info).is_none() {
                    debug!("added new peer to handler {}", node_id);

                    // Attempt joining network when trying for the first time
                    if !self.has_joined_topic(self.network_id).await? {
                        self.subscribe_to_network().await?;
                    }
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

        Ok(())
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
            .choose_multiple(&mut rand::thread_rng(), JOIN_PEERS_SAMPLE_LEN)
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

    async fn subscribe_to_topics(&mut self) -> Result<()> {
        for topic in self.pending_subscriptions.keys() {
            self.subscribe_to_topic(*topic).await?;
        }

        Ok(())
    }

    async fn subscribe_to_topic(&self, topic: TopicId) -> Result<()> {
        if self.has_joined_topic(topic).await? {
            return Ok(());
        }

        if let Some(known_topic) = self.known_topics.get(&topic) {
            let peers = known_topic
                .to_owned()
                .into_iter()
                .choose_multiple(&mut rand::thread_rng(), JOIN_PEERS_SAMPLE_LEN);

            if peers.is_empty() {
                return Ok(());
            }

            println!("JOIN TOPIC {}", topic);

            self.gossip_actor_tx
                .send(ToGossipActor::Join { topic, peers })
                .await?;
        }

        Ok(())
    }

    fn pending_subscribe(
        &mut self,
        topic: TopicId,
        out_tx: broadcast::Sender<OutEvent>,
        mut in_rx: mpsc::Receiver<InEvent>,
    ) {
        if self.subscriptions.contains_key(&topic) {
            return;
        }

        if self.pending_subscriptions.contains_key(&topic) {
            return;
        }

        // @TODO: Refactor this, this is just a POC hack, we want the whole subscription logic to
        // live in its own struct
        let gossip = self.gossip.clone();
        let gossip_actor_tx = self.gossip_actor_tx.clone();
        tokio::task::spawn(async move {
            while let Some(event) = in_rx.recv().await {
                let (reply, reply_rx) = oneshot::channel();
                gossip_actor_tx
                    .send(ToGossipActor::HasJoined { topic, reply })
                    .await
                    .ok();
                let result = reply_rx.await.unwrap().unwrap();
                if !result {
                    continue;
                }

                match event {
                    InEvent::Message { bytes } => {
                        gossip
                            .broadcast_neighbors(topic, bytes.into())
                            .await
                            .unwrap();
                    }
                }
            }
        });

        self.pending_subscriptions.insert(topic, out_tx);
    }

    async fn announce_topics(&mut self) -> Result<()> {
        let mut topics = Vec::new();
        topics.extend(self.pending_subscriptions.keys());
        topics.extend(self.subscriptions.keys());
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

    async fn has_joined_topic(&self, topic: TopicId) -> Result<bool> {
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
        if topic == self.network_id {
            // Message coming from network-wide gossip overlay
            let message = NetworkMessage::from_bytes(&bytes)
                .context("parsing network-wide gossip message")?;
            match message {
                NetworkMessage::Announcement(topics) => {
                    self.on_announcement_message(topics, delivered_from);
                }
            }
        // @TODO: It's wrong that we're checking pending subscriptions here. We need to move
        // pending into active subscriptions at one point!
        } else if self.pending_subscriptions.contains_key(&topic) {
            // Message coming from a subscribed topic
            let out_tx = self.pending_subscriptions.get(&topic).unwrap();
            out_tx.send(OutEvent::Message {
                bytes,
                // @TODO: Do this more efficiently
                delivered_from: PandaPublicKey::from_bytes(delivered_from.as_bytes())?,
            })?;
        } else {
            debug!("Received message for unknown topic {}", topic);
        }

        Ok(())
    }

    fn on_announcement_message(&mut self, topics: Vec<TopicId>, delivered_from: PublicKey) {
        debug!(
            "Received announcement of peer {} {:?}",
            delivered_from, topics
        );

        for topic in &topics {
            match self.known_topics.get_mut(topic) {
                Some(node_ids) => {
                    node_ids.push(delivered_from);
                }
                None => {
                    self.known_topics.insert(*topic, vec![delivered_from]);
                }
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
