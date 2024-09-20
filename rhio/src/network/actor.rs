use std::collections::{hash_map, HashMap, HashSet};
use std::future::Future;
use std::pin::Pin;

use anyhow::{anyhow, Result};
use futures_util::FutureExt;
use p2panda_core::{Body, Header, Operation};
use p2panda_engine::operation::{ingest_operation, IngestResult};
use p2panda_engine::{DecodeExt, IngestExt};
use p2panda_net::network::{InEvent, OutEvent};
use p2panda_net::Network;
use p2panda_store::MemoryStore;
use rhio_core::{encode_operation, RhioExtensions, TopicId};
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::{Stream, StreamExt, StreamMap};
use tracing::{error, trace};

pub type SubscribeResult = Result<(
    broadcast::Receiver<Operation<RhioExtensions>>,
    Pin<Box<dyn Future<Output = ()> + Send>>,
)>;

pub type TopicEventStreams = StreamMap<
    TopicId,
    Pin<Box<dyn Stream<Item = Result<OutEvent, BroadcastStreamRecvError>> + Send + 'static>>,
>;

pub type TopicOperationStreams =
    StreamMap<TopicId, Pin<Box<dyn Stream<Item = Operation<RhioExtensions>> + Send + 'static>>>;

pub enum ToPandaActor {
    Ingest {
        header: Header<RhioExtensions>,
        body: Option<Body>,
        reply: oneshot::Sender<Result<Operation<RhioExtensions>>>,
    },
    Broadcast {
        topic: TopicId,
        header: Header<RhioExtensions>,
        body: Option<Body>,
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
    /// p2panda-net network.
    network: Network,

    /// In memory p2panda store.
    store: MemoryStore<[u8; 32], RhioExtensions>,

    /// Map containing senders for all subscribed topics. Messages sent on this channels will be
    /// broadcast to peers interested in the same topic.
    topic_gossip_tx_map: HashMap<TopicId, mpsc::Sender<InEvent>>,

    /// Map of newly subscribed topic streams which have not received their `Ready` message yet.
    /// Once the `Ready` message arrives the stream is moved to the `topic_streams` map.
    pre_ready_topic_streams: TopicEventStreams,

    /// Map of p2panda "engine" streams. Decoded and validated and in-order p2panda operations are
    /// received on these streams.
    topic_streams: TopicOperationStreams,

    /// Map of broadcast senders where newly arrived p2panda operations will be routed.
    topic_subscribers_tx_map: HashMap<TopicId, broadcast::Sender<Operation<RhioExtensions>>>,

    /// Channel used for broadcasting "topic joined" notification events.
    broadcast_join: broadcast::Sender<TopicId>,

    /// Set containing all successfully joined topics.
    joined_topics: HashSet<TopicId>,

    /// Actor inbox.
    inbox: mpsc::Receiver<ToPandaActor>,
}

impl PandaActor {
    pub fn new(
        network: Network,
        store: MemoryStore<[u8; 32], RhioExtensions>,
        inbox: mpsc::Receiver<ToPandaActor>,
    ) -> Self {
        let (broadcast_join, _) = broadcast::channel::<TopicId>(128);

        Self {
            network,
            store,
            topic_gossip_tx_map: HashMap::default(),
            pre_ready_topic_streams: StreamMap::default(),
            topic_streams: StreamMap::default(),
            topic_subscribers_tx_map: HashMap::new(),
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
                Some((topic, result)) = self.pre_ready_topic_streams.next() => {
                    let result = match result {
                        Ok(event) => self.on_topic_event(topic, event).await,
                        Err(e) => break Err(anyhow!(e))
                    };

                    if let Err(e) = result {
                        break Err(e)
                    }

                }
                Some((topic_id, msg)) = self.topic_streams.next() => {
                    self
                        .on_topic_operation(topic_id, msg)
                        .await;
                }
            }
        }
    }

    async fn on_actor_message(&mut self, msg: ToPandaActor) -> Result<bool> {
        match msg {
            ToPandaActor::Ingest {
                header,
                body,
                reply,
            } => {
                let result = self.on_ingest(header, body).await;
                reply.send(result).ok();
            }
            ToPandaActor::Broadcast {
                header,
                body,
                topic,
                reply,
            } => {
                let result = self.on_broadcast(header, body, topic).await;
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

    async fn on_broadcast(
        &mut self,
        header: Header<RhioExtensions>,
        body: Option<Body>,
        topic: TopicId,
    ) -> Result<()> {
        match self.topic_gossip_tx_map.get_mut(&topic) {
            Some(tx) => {
                let bytes = encode_operation(header, body)?;
                tx.send(InEvent::Message { bytes }).await
            }
            None => {
                return Err(anyhow!(
                    "attempted to send operation on unknown topic {topic:?}"
                ))
            }
        }?;
        Ok(())
    }

    async fn on_subscribe(
        &mut self,
        topic: TopicId,
    ) -> Result<(
        broadcast::Receiver<Operation<RhioExtensions>>,
        Pin<Box<dyn Future<Output = ()> + Send>>,
    )> {
        let (topic_tx, topic_rx) = self.network.subscribe(topic.into()).await?;

        // If we didn't already subscribe to this topic we need to insert the receiver into the
        // pre_ready_topic_streams map, where it will stay until a `Ready` message arrives, and
        // insert the sender into topic_tx map, ready to handle broadcasting of messages.
        if let hash_map::Entry::Vacant(entry) = self.topic_gossip_tx_map.entry(topic) {
            let stream = BroadcastStream::new(topic_rx);
            self.pre_ready_topic_streams.insert(topic, Box::pin(stream));

            entry.insert(topic_tx);
        }

        // Get a receiver where decoded, validated and ordered operations arriving on this topic will be sent.
        let rx = if let Some(tx) = self.topic_subscribers_tx_map.get(&topic) {
            tx.subscribe()
        } else {
            let (tx, rx) = broadcast::channel(128);
            self.topic_subscribers_tx_map.insert(topic, tx);
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
            while let Ok(joined_topic) = joined_rx.recv().await {
                if joined_topic == topic {
                    return;
                }
            }
        };

        Ok((rx, fut.boxed()))
    }

    async fn on_ingest(
        &mut self,
        header: Header<RhioExtensions>,
        body: Option<Body>,
    ) -> Result<Operation<RhioExtensions>> {
        trace!(
            "ingest operation from {} @ {}",
            header.public_key,
            header.seq_num
        );
        match ingest_operation(&mut self.store, header, body).await {
            Ok(IngestResult::Complete(operation)) => Ok(operation),
            Ok(IngestResult::Retry(_, _, _)) => {
                return Err(anyhow!("Unexpected out-of-order operation received"))
            }
            Err(e) => return Err(anyhow!(e)),
        }
    }

    async fn on_topic_operation(&mut self, topic: TopicId, operation: Operation<RhioExtensions>) {
        if let Some(rx) = self.topic_subscribers_tx_map.get(&topic) {
            rx.send(operation).expect("topic channel closed");
        }
    }

    async fn on_topic_event(&mut self, topic: TopicId, event: OutEvent) -> Result<()> {
        match event {
            OutEvent::Ready => {
                // Take the stream from the "pre-ready" map so we can process it and later move it
                // to the main topic_streams map.
                let stream = self
                    .pre_ready_topic_streams
                    .remove(&topic)
                    .expect("topic stream not found");

                // Process the stream so items contain expected operation header and option body bytes.
                let stream = stream.filter_map(|event| match event {
                    Ok(OutEvent::Ready) => None,
                    Ok(OutEvent::Message { bytes, .. }) => {
                        let raw_operation: Result<(Vec<u8>, Option<Vec<u8>>), _> =
                            ciborium::from_reader(&bytes[..]);
                        match raw_operation {
                            Ok(data) => Some(data),
                            Err(err) => {
                                eprintln!("failed deserializing JSON: {err}");
                                None
                            }
                        }
                    }
                    Err(_) => None,
                });

                // Decode items in the stream to Header and Body tuples.
                let stream = stream.decode().filter_map(|event| match event {
                    Ok((header, body)) => Some((header, body)),
                    Err(err) => {
                        eprintln!("failed decoding operation: {err}");
                        None
                    }
                });

                // Ingest all operations, buffering out-of-order arrivals when required.
                let stream =
                    stream
                        .ingest(self.store.clone(), 128)
                        .filter_map(|event| match event {
                            Ok(operation) => Some(operation),
                            Err(err) => {
                                eprintln!("failed ingesting operation: {err}");
                                None
                            }
                        });

                // Place the processed stream into the topic_streams map.
                let stream = Box::pin(stream);
                self.topic_streams.insert(topic, stream);

                // Flag that the topic was successfully joined.
                self.joined_topics.insert(topic);

                // Announce the successful join.
                self.broadcast_join
                    .send(topic)
                    .expect("broadcast_join channel not dropped");
                Ok(())
            }
            OutEvent::Message { .. } => {
                return Err(anyhow!("OutEvent::Message received before OutEvent::Ready"))
            }
        }
    }

    async fn shutdown(self) -> Result<()> {
        self.network.shutdown().await?;
        Ok(())
    }
}
