use std::collections::HashMap;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::pin::Pin;

use anyhow::Result;
use p2panda_blobs::{Blobs, MemoryStore as BlobMemoryStore};
use p2panda_core::{PrivateKey, PublicKey};
use p2panda_net::config::DEFAULT_STUN_PORT;
use p2panda_net::network::{InEvent, OutEvent};
use p2panda_net::{LocalDiscovery, Message as TopicMessage, Network, NetworkBuilder};
use p2panda_store::MemoryStore as LogMemoryStore;
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::task::JoinHandle;
use tokio_stream::{Stream, StreamMap};
use tracing::error;

use crate::actor::{RhioActor, ToRhioActor};
use crate::config::Config;
use crate::messages::Message;
use crate::topic_id::TopicId;
use crate::{BLOB_ANNOUNCE_TOPIC, FILE_SYSTEM_EVENT_TOPIC};

pub struct Node {
    config: Config,
    network: Network,
    rhio_actor_tx: mpsc::Sender<ToRhioActor>,
    actor_handle: JoinHandle<()>,
    ready_rx: mpsc::Receiver<()>,
}

impl Node {
    pub async fn spawn(config: Config, private_key: PrivateKey) -> Result<Self> {
        let (rhio_actor_tx, rhio_actor_rx) = mpsc::channel(256);
        let (ready_tx, ready_rx) = mpsc::channel::<()>(1);

        let blob_store = BlobMemoryStore::new();
        let log_store = LogMemoryStore::default();

        let mut network_builder = NetworkBuilder::from_config(config.network_config.clone())
            .private_key(private_key.clone());

        match LocalDiscovery::new() {
            Ok(local) => network_builder = network_builder.discovery(local),
            Err(err) => error!("Failed to initiate local discovery: {err}"),
        }

        for relay_addr in &config.network_config.relay_addresses {
            network_builder =
                network_builder.relay(relay_addr.to_owned(), false, DEFAULT_STUN_PORT);
        }

        let (mut network, blobs) = Blobs::from_builder(network_builder, blob_store).await?;

        let mut gossip_tx_map = HashMap::new();
        let mut gossip_rx_stream_map = StreamMap::new();
        add_topic(
            &mut network,
            BLOB_ANNOUNCE_TOPIC,
            &mut gossip_tx_map,
            &mut gossip_rx_stream_map,
        )
        .await
        .expect("can subscribe topic");

        add_topic(
            &mut network,
            FILE_SYSTEM_EVENT_TOPIC,
            &mut gossip_tx_map,
            &mut gossip_rx_stream_map,
        )
        .await
        .expect("can subscribe topic");

        let mut rhio_actor = RhioActor::new(
            blobs.clone(),
            config.blobs_path.clone(),
            private_key.clone(),
            log_store,
            gossip_tx_map,
            gossip_rx_stream_map,
            rhio_actor_rx,
            ready_tx,
        );

        let actor_handle = tokio::task::spawn(async move {
            if let Err(err) = rhio_actor.run().await {
                panic!("operations actor failed: {err:?}");
            }
        });

        let node = Node {
            config,
            network,
            rhio_actor_tx,
            actor_handle,
            ready_rx,
        };

        Ok(node)
    }

    pub async fn direct_addresses(&self) -> Option<Vec<SocketAddr>> {
        self.network.direct_addresses().await
    }

    pub fn id(&self) -> PublicKey {
        self.network.node_id()
    }

    pub async fn publish_event(&self, log_id: String, bytes: Vec<u8>) -> Result<()> {
        let topic = TopicId::from_str(&log_id);
        let (reply, reply_rx) = oneshot::channel();
        self.rhio_actor_tx
            .send(ToRhioActor::PublishEvent {
                topic,
                bytes,
                reply,
            })
            .await?;
        reply_rx.await?
    }

    pub async fn import_blob(&self, path: PathBuf) -> Result<()> {
        let (reply, reply_rx) = oneshot::channel();
        self.rhio_actor_tx
            .send(ToRhioActor::ImportBlob { path, reply })
            .await?;
        reply_rx.await?
    }

    pub async fn sync_file(&self, absolute_path: PathBuf) -> Result<()> {
        // We derive the relative path by stripping off the base blobs config path. This is done
        // as we want to import by absolute path, but announce relative paths (relative to the
        // configured blob directory).
        let relative_path = to_relative_path(&absolute_path, &self.config.blobs_path);
        let (reply, reply_rx) = oneshot::channel();
        self.rhio_actor_tx
            .send(ToRhioActor::SyncFile {
                absolute_path,
                relative_path,
                reply,
            })
            .await?;
        reply_rx.await?
    }

    pub async fn subscribe<T>(&self, topic: TopicId) -> Result<(RhioSender<T>, RhioReceiver<T>)>
    where
        T: TopicMessage + Send + Sync + 'static,
    {
        let (out_tx, out_rx) = broadcast::channel(128);
        let (reply, reply_rx) = oneshot::channel();
        self.rhio_actor_tx
            .send(ToRhioActor::Subscribe {
                topic,
                out_tx,
                reply,
            })
            .await?;
        let _ = reply_rx.await?;
        let tx = RhioSender::new(topic, self.rhio_actor_tx.clone());
        let rx = RhioReceiver::new(out_rx);
        Ok((tx, rx))
    }

    pub async fn ready(&mut self) -> Option<()> {
        self.ready_rx.recv().await
    }

    pub async fn shutdown(self) -> Result<()> {
        // Trigger shutdown of the main run task by activating the cancel token
        self.rhio_actor_tx.send(ToRhioActor::Shutdown).await?;
        self.network.shutdown().await?;
        self.actor_handle.await?;
        Ok(())
    }
}

fn to_relative_path(path: &PathBuf, base: &PathBuf) -> PathBuf {
    path.strip_prefix(base)
        .expect("Blob import path contains blob dir")
        .to_path_buf()
}

pub struct RhioSender<T: TopicMessage> {
    topic_id: TopicId,
    tx: mpsc::Sender<ToRhioActor>,
    _phantom: PhantomData<T>,
}

impl<T> RhioSender<T>
where
    T: TopicMessage + Send + Sync + 'static,
{
    pub fn new(topic_id: TopicId, tx: mpsc::Sender<ToRhioActor>) -> RhioSender<T> {
        RhioSender {
            topic_id,
            tx,
            _phantom: PhantomData::<T>,
        }
    }
    pub async fn send(&self, message: T) -> Result<()> {
        let (reply, reply_rx) = oneshot::channel();
        self.tx
            .send(ToRhioActor::PublishEvent {
                topic: self.topic_id,
                bytes: message.to_bytes(),
                reply,
            })
            .await?;
        reply_rx.await?
    }
}

pub struct RhioReceiver<T: TopicMessage> {
    rx: broadcast::Receiver<Message>,
    _phantom: PhantomData<T>,
}

impl<T: TopicMessage> RhioReceiver<T> {
    fn new(rx: broadcast::Receiver<Message>) -> RhioReceiver<T> {
        Self {
            rx,
            _phantom: PhantomData::<T>,
        }
    }

    pub async fn recv(&mut self) -> Result<Message> {
        match self.rx.recv().await {
            Ok(message) => Ok(message),
            Err(err) => Err(anyhow::anyhow!(err)),
        }
    }
}

async fn add_topic(
    network: &mut Network,
    topic_str: &str,
    tx_map: &mut HashMap<TopicId, mpsc::Sender<InEvent>>,
    rx_streams_map: &mut StreamMap<TopicId, Pin<Box<dyn Stream<Item = OutEvent> + Send + 'static>>>,
) -> Result<()> {
    let topic = TopicId::from_str(topic_str);
    let (tx, mut rx) = network.subscribe(topic.into()).await?;
    tx_map.insert(topic, tx);

    let rx_stream = Box::pin(async_stream::stream! {
      while let Ok(item) = rx.recv().await {
          yield item;
      }
    }) as Pin<Box<dyn Stream<Item = OutEvent> + Send>>;
    rx_streams_map.insert(topic, rx_stream);
    Ok(())
}
