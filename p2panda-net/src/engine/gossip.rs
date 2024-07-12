// SPDX-License-Identifier: AGPL-3.0-or-later

use std::collections::HashSet;

use anyhow::{Context, Result};
use futures_lite::StreamExt;
use futures_util::FutureExt;
use iroh_gossip::net::Event;
use iroh_gossip::net::Gossip;
use iroh_gossip::proto::TopicId;
use iroh_net::key::PublicKey;
use tokio::sync::oneshot;
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinSet;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamMap;
use tracing::{error, warn};

use super::engine::ToEngineActor;

type Reply<T> = oneshot::Sender<Result<T>>;

#[derive(Debug)]
pub enum ToGossipActor {
    Join {
        topic: TopicId,
        peers: Vec<PublicKey>,
    },
    HasJoined {
        topic: TopicId,
        reply: Reply<bool>,
    },
    Leave {
        topic: TopicId,
    },
    Shutdown,
}

pub struct GossipActor {
    inbox: mpsc::Receiver<ToGossipActor>,
    gossip: Gossip,
    engine_actor_tx: mpsc::Sender<ToEngineActor>,
    joined: HashSet<TopicId>,
    want_join: HashSet<TopicId>,
    pending_joins: JoinSet<(TopicId, Result<broadcast::Receiver<Event>>)>,
    gossip_events: StreamMap<TopicId, BroadcastStream<Event>>,
}

impl GossipActor {
    pub fn new(
        inbox: mpsc::Receiver<ToGossipActor>,
        gossip: Gossip,
        engine_actor_tx: mpsc::Sender<ToEngineActor>,
    ) -> Self {
        Self {
            inbox,
            gossip,
            engine_actor_tx,
            joined: Default::default(),
            want_join: Default::default(),
            pending_joins: Default::default(),
            gossip_events: Default::default(),
        }
    }

    pub async fn run(&mut self) -> Result<()> {
        loop {
            tokio::select! {
                next = self.gossip_events.next(), if !self.gossip_events.is_empty() => {
                    if let Err(err) = self.on_gossip_event(next).await {
                        error!("gossip actor died: {err:?}");
                        return Err(err);
                    }
                },
                msg = self.inbox.recv() => {
                    let msg = msg.context("to_actor closed")?;
                    if !self.on_actor_message(msg).await.context("on_actor_message")? {
                        break;
                    }
                }
                Some(res) = self.pending_joins.join_next(), if !self.pending_joins.is_empty() => {
                    let (topic, res) = res.context("pending_joins closed")?;
                    match res {
                        Ok(stream) => {
                            self.joined.insert(topic);
                            let stream = BroadcastStream::new(stream);
                            self.gossip_events.insert(topic, stream);
                        },
                        Err(err) => {
                            if self.want_join.contains(&topic) {
                                error!(?topic, ?err, "failed to join gossip");
                            }
                        }
                    }
                }

            }
        }

        Ok(())
    }

    async fn on_actor_message(&mut self, msg: ToGossipActor) -> Result<bool> {
        match msg {
            ToGossipActor::Join { topic, peers } => {
                let gossip = self.gossip.clone();
                let fut = async move {
                    let stream = gossip.subscribe(topic.into()).await?;
                    let _topic = gossip.join(topic.into(), peers).await?.await?;
                    Ok(stream)
                };
                let fut = fut.map(move |res| (topic, res));
                self.want_join.insert(topic);
                self.pending_joins.spawn(fut);
            }
            ToGossipActor::HasJoined { topic, reply } => {
                let result = self.joined.contains(&topic);
                reply.send(Ok(result)).ok();
            }
            ToGossipActor::Leave { topic } => {
                self.gossip.quit(topic.into()).await?;
                self.joined.remove(&topic);
                self.want_join.remove(&topic);
            }
            ToGossipActor::Shutdown => {
                for topic in self.joined.iter() {
                    self.gossip.quit((*topic).into()).await.ok();
                }
                return Ok(false);
            }
        }

        Ok(true)
    }

    async fn on_gossip_event(
        &mut self,
        event: Option<(TopicId, Result<Event, BroadcastStreamRecvError>)>,
    ) -> Result<()> {
        let (topic, event) = event.context("Gossip event channel closed")?;
        let event = match event {
            Ok(event) => event,
            Err(BroadcastStreamRecvError::Lagged(n)) => {
                warn!("GossipActor too slow (lagged by {n}) - dropping gossip event");
                return Ok(());
            }
        };

        if !self.joined.contains(&topic) && !self.want_join.contains(&topic) {
            error!(topic = %topic, "received gossip event for unknown topic");
            return Ok(());
        }

        if let Err(err) = self.on_gossip_event_inner(topic, event).await {
            error!(topic = %topic, ?err, "Failed to process gossip event");
        }

        Ok(())
    }

    async fn on_gossip_event_inner(&mut self, topic: TopicId, event: Event) -> Result<()> {
        match event {
            Event::Received(msg) => {
                self.engine_actor_tx
                    .send(ToEngineActor::Received {
                        bytes: msg.content.into(),
                        delivered_from: msg.delivered_from,
                        topic,
                    })
                    .await?;
            }
            Event::NeighborUp(peer) => {
                self.engine_actor_tx
                    .send(ToEngineActor::NeighborUp { topic, peer })
                    .await?;
            }
            Event::NeighborDown(peer) => {
                self.engine_actor_tx
                    .send(ToEngineActor::NeighborDown { topic, peer })
                    .await?;
            }
        }
        Ok(())
    }
}
