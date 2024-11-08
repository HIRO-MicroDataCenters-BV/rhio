use std::collections::HashMap;
use std::pin::Pin;

use anyhow::Result;
use p2panda_core::{Body, Header, Operation};
use p2panda_net::network::ToNetwork;
use p2panda_net::Network;
use p2panda_store::MemoryStore;
use rhio_core::{LogId, RhioExtensions, TopicId};
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio_stream::{Stream, StreamExt, StreamMap};
use tracing::error;

use crate::topic::{Query, Subscription};

pub enum ToPandaActor {
    Ingest {
        header: Header<RhioExtensions>,
        body: Option<Body>,
        reply: oneshot::Sender<Result<Operation<RhioExtensions>>>,
    },
    Broadcast {
        payload: Vec<u8>,
        topic_id: [u8; 32],
        reply: oneshot::Sender<Result<()>>,
    },
    Subscribe {
        query: Query,
        reply: oneshot::Sender<broadcast::Receiver<Operation<RhioExtensions>>>,
    },
    Shutdown {
        reply: oneshot::Sender<()>,
    },
}

pub struct PandaActor {
    /// p2panda-net network.
    network: Network<Query>,

    /// In memory p2panda store.
    store: MemoryStore<LogId, RhioExtensions>,

    /// Map of p2panda "engine" streams. Decoded, validated and in-order p2panda operations are
    /// received on these streams.
    topic_streams:
        StreamMap<TopicId, Pin<Box<dyn Stream<Item = Operation<RhioExtensions>> + Send + 'static>>>,

    /// Map containing senders for all subscribed topics. Messages sent on this channels will be
    /// broadcast to peers interested in the same topic.
    topic_gossip_tx_map: HashMap<TopicId, mpsc::Sender<ToNetwork>>,

    /// Map of subscription senders where newly arrived p2panda operations will be routed.
    topic_subscribers_tx_map: HashMap<TopicId, broadcast::Sender<Operation<RhioExtensions>>>,

    /// Actor inbox.
    inbox: mpsc::Receiver<ToPandaActor>,
}

impl PandaActor {
    pub fn new(
        network: Network<Query>,
        store: MemoryStore<LogId, RhioExtensions>,
        inbox: mpsc::Receiver<ToPandaActor>,
    ) -> Self {
        Self {
            network,
            store,
            topic_gossip_tx_map: HashMap::default(),
            topic_streams: StreamMap::default(),
            topic_subscribers_tx_map: HashMap::new(),
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
                Some((topic_id, msg)) = self.topic_streams.next() => {
                    // self
                    //     .on_topic_operation(topic_id, msg)
                    //     .await;
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
                // let result = self.on_ingest(header, body).await;
                // reply.send(result).ok();
            }
            ToPandaActor::Broadcast {
                payload,
                topic_id,
                reply,
            } => {
                let result = self.on_broadcast(payload, topic_id).await;
                reply.send(result).ok();
            }
            ToPandaActor::Subscribe { query, reply } => {
                let result = self.on_subscribe(query).await?;
                reply.send(result).ok();
            }
            ToPandaActor::Shutdown { .. } => {
                unreachable!("handled in run_inner");
            }
        }

        Ok(true)
    }

    async fn on_broadcast(&mut self, payload: Vec<u8>, topic_id: [u8; 32]) -> Result<()> {
        // match self.topic_gossip_tx_map.get_mut(&topic) {
        //     Some(tx) => {
        //         let bytes = encode_operation(header, body)?;
        //         tx.send(ToNetwork::Message { bytes }).await
        //     }
        //     None => {
        //         return Err(anyhow!(
        //             "attempted to send operation on unknown topic {topic:?}"
        //         ))
        //     }
        // }?;
        Ok(())
    }

    async fn on_subscribe(
        &mut self,
        query: Query,
    ) -> Result<broadcast::Receiver<Operation<RhioExtensions>>> {
        let (topic_tx, topic_rx, _) = self.network.subscribe(query).await?;

        //
        // // If we didn't already subscribe to this topic we need to insert the `tx` into the topic
        // // gossip map where we'll use it for broadcasting operations on the network, and we need
        // // to convert the `rx` into a p2panda engine stream and insert it into the topic streams
        // // map and listen for incoming operations.
        // if let hash_map::Entry::Vacant(entry) = self.topic_gossip_tx_map.entry(topic) {
        //     entry.insert(topic_tx);
        //
        //     let stream = BroadcastStream::new(topic_rx);
        //
        //     // Process the stream so items contain expected operation header and option body bytes.
        //     let stream = stream.filter_map(|event| match event {
        //         Ok(FromNetwork::Ready) => None,
        //         Ok(FromNetwork::GossipMessage { bytes, .. }) => {
        //             let operation = decode_operation(&bytes[..]);
        //             match operation {
        //                 Ok((header, body)) => Some((header, body)),
        //                 Err(err) => {
        //                     error!("failed deserializing CBOR: {err}");
        //                     None
        //                 }
        //             }
        //         }
        //         Err(_) => None,
        //     });

        // Ingest all operations, buffering out-of-order arrivals when required.
        // let stream = stream
        //     .ingest(self.store.clone(), 128)
        //     .filter_map(|event| match event {
        //         Ok(operation) => Some(operation),
        //         Err(err) => {
        //             error!("failed ingesting operation: {err}");
        //             None
        //         }
        //     });
        //
        // // Place the processed stream into the topic_streams map.
        // let stream = Box::pin(stream);
        // self.topic_streams.insert(topic, stream);
        // }

        // Get a receiver where decoded, validated and ordered operations arriving on this topic will be sent.
        // let rx = if let Some(tx) = self.topic_subscribers_tx_map.get(&topic) {
        //     tx.subscribe()
        // } else {
        //     let (tx, rx) = broadcast::channel(128);
        //     self.topic_subscribers_tx_map.insert(topic, tx);
        //     rx
        // };
        //
        // Ok(rx)
        todo!()
    }

    async fn shutdown(self) -> Result<()> {
        self.network.shutdown().await?;
        Ok(())
    }
}
