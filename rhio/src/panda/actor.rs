use std::collections::{hash_map, HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;
use std::time::{self, SystemTime};

use anyhow::Result;
use futures_lite::FutureExt;
use p2panda_core::Operation;
use p2panda_net::network::{InEvent, OutEvent};
use p2panda_net::Network;
use p2panda_store::{MemoryStore, OperationStore};
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio_stream::{Stream, StreamExt, StreamMap};
use tracing::{debug, error};

use crate::panda::extensions::{LogId, RhioExtensions};
use crate::panda::messages::{FromBytes, GossipOperation, Message, MessageMeta, ToBytes};
use crate::panda::operations::ingest_operation;
use crate::panda::topic_id::TopicId;

pub type SubscribeResult = Result<(
    broadcast::Receiver<(Message, MessageMeta)>,
    Pin<Box<dyn Future<Output = ()> + Send>>,
)>;

pub enum ToPandaActor {
    Ingest {
        topic: TopicId,
        operation: Operation<RhioExtensions>,
        reply: oneshot::Sender<Result<()>>,
    },
    Subscribe {
        topic: TopicId,
        reply: oneshot::Sender<SubscribeResult>,
    },
    Shutdown {
        reply: oneshot::Sender<()>,
    },
}

pub struct PandaActor {
    network: Network,
    store: MemoryStore<LogId, RhioExtensions>,
    topic_gossip_tx: HashMap<TopicId, mpsc::Sender<InEvent>>,
    topic_topic_rx: StreamMap<TopicId, Pin<Box<dyn Stream<Item = OutEvent> + Send + 'static>>>,
    topic_subscribers_tx: HashMap<TopicId, broadcast::Sender<(Message, MessageMeta)>>,
    broadcast_join: broadcast::Sender<TopicId>,
    joined_topics: HashSet<TopicId>,
    inbox: mpsc::Receiver<ToPandaActor>,
}

impl PandaActor {
    pub fn new(network: Network, inbox: mpsc::Receiver<ToPandaActor>) -> Self {
        let store: MemoryStore<LogId, RhioExtensions> = MemoryStore::new();
        let (broadcast_join, _) = broadcast::channel::<TopicId>(128);

        Self {
            network,
            store,
            topic_gossip_tx: HashMap::default(),
            topic_topic_rx: StreamMap::default(),
            topic_subscribers_tx: HashMap::new(),
            broadcast_join,
            joined_topics: HashSet::new(),
            inbox,
        }
    }

    pub async fn run(mut self) -> Result<()> {
        // Take oneshot sender from outside API awaited by `shutdown` call and fire it as soon as
        // shutdown completed
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
                                break Err(err);
                            }
                        }
                    }
                },
                Some((topic_id, msg)) = self.topic_topic_rx.next() => {
                    self
                        .on_gossip_event(topic_id, msg)
                        .await;
                }
            }
        }
    }

    async fn send_operation(&mut self, topic: TopicId, operation: GossipOperation) -> Result<()> {
        match self.topic_gossip_tx.get_mut(&topic) {
            Some(tx) => {
                tx.send(InEvent::Message {
                    bytes: operation.to_bytes(),
                })
                .await
            }
            None => {
                return Err(anyhow::anyhow!(
                    "Attempted to send operation on unknown topic {topic:?}"
                ))
            }
        }?;
        Ok(())
    }

    async fn on_actor_message(&mut self, msg: ToPandaActor) -> Result<bool> {
        match msg {
            ToPandaActor::Ingest {
                topic,
                operation,
                reply,
            } => {
                let result = self.on_ingest(topic, operation).await;
                reply.send(result).ok();
            }
            ToPandaActor::Subscribe { topic, reply } => {
                let result = self.on_subscribe(topic).await;
                reply.send(result).ok();
            }
            ToPandaActor::Shutdown { .. } => {
                unreachable!("handled in run_inner");
            }
        }

        Ok(true)
    }

    async fn on_subscribe(
        &mut self,
        topic: TopicId,
    ) -> Result<(
        broadcast::Receiver<(Message, MessageMeta)>,
        Pin<Box<dyn Future<Output = ()> + Send>>,
    )> {
        let (topic_tx, mut topic_rx) = self.network.subscribe(topic.into()).await?;

        // If we didn't already subscribe to this topic, then add the topic gossip channels to our
        // sender and receiver maps
        if let hash_map::Entry::Vacant(entry) = self.topic_gossip_tx.entry(topic) {
            entry.insert(topic_tx);

            let rx_stream = Box::pin(async_stream::stream! {
              while let Ok(item) = topic_rx.recv().await {
                  yield item;
              }
            });

            self.topic_topic_rx.insert(topic, rx_stream);
        }

        // Get a receiver channel which will be sent decoded gossip events arriving on this topic
        let rx = if let Some(tx) = self.topic_subscribers_tx.get(&topic) {
            tx.subscribe()
        } else {
            let (tx, rx) = broadcast::channel(128);
            self.topic_subscribers_tx.insert(topic, tx);
            rx
        };

        // Subscribe to the broadcast channel which receives "topic joined" events
        let mut joined_rx = self.broadcast_join.subscribe();

        // Flag if the topic has already been joined
        let has_joined = self.joined_topics.contains(&topic);

        // Future which returns when the topic has been joined (or is already joined)
        let fut = async move {
            if has_joined {
                return;
            }
            loop {
                let joined_topic = joined_rx.recv().await.expect("channel is not dropped");
                if joined_topic == topic {
                    return;
                }
            }
        };

        Ok((rx, fut.boxed()))
    }

    async fn on_ingest(
        &mut self,
        topic: TopicId,
        operation: Operation<RhioExtensions>,
    ) -> Result<()> {
        // The log id is {PUBLIC_KEY}/{TOPIC_ID} string.
        let log_id = format!("{}/{}", operation.header.public_key.to_hex(), topic);
        self.store
            .insert_operation(operation.clone(), log_id.to_owned())?;
        Ok(())
    }

    async fn on_gossip_event(&mut self, topic: TopicId, event: OutEvent) {
        match event {
            OutEvent::Ready => {
                self.joined_topics.insert(topic);
                self.broadcast_join
                    .send(topic)
                    .expect("broadcast_join channel not dropped");
            }
            OutEvent::Message {
                bytes,
                delivered_from,
            } => {
                let operation = match GossipOperation::from_bytes(&bytes) {
                    Ok(operation) => operation,
                    Err(err) => {
                        error!("failed to decode gossip operation: {err}");
                        return;
                    }
                };

                match ingest_operation(
                    &mut self.store,
                    operation.header.clone(),
                    Some(operation.body()),
                ) {
                    Ok(result) => result,
                    Err(err) => {
                        error!("failed to ingest operation from {delivered_from}: {err}");
                        return;
                    }
                };

                debug!(
                    "Received operation: {} {} {} {}",
                    operation.header.public_key,
                    operation.header.seq_num,
                    operation.header.timestamp,
                    operation.header.hash(),
                );

                let message_context = MessageMeta {
                    operation_timestamp: operation.header.timestamp,
                    delivered_from,
                    received_at: time::SystemTime::now()
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .expect("can calculate duration since UNIX_EPOCH")
                        .as_secs(),
                };

                let tx = self
                    .topic_subscribers_tx
                    .get(&topic)
                    .expect("topic is known");

                let _ = tx.send((operation.message.clone(), message_context));
            }
        }
    }

    async fn shutdown(self) -> Result<()> {
        self.network.shutdown().await?;
        Ok(())
    }
}