mod actor;

use std::net::SocketAddr;

use anyhow::{anyhow, Result};
use futures_util::future::{MapErr, Shared};
use futures_util::{FutureExt, TryFutureExt};
use p2panda_blobs::{Blobs as BlobsHandler, MemoryStore as BlobsMemoryStore};
use p2panda_core::{Hash, PrivateKey, PublicKey};
use p2panda_net::{Config as NetworkConfig, NetworkBuilder};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinError;
use tokio_util::task::AbortOnDropHandle;
use tracing::error;

use crate::blobs::Blobs;
use crate::config::Config;
use crate::nats::Nats;
use crate::network::sync::RhioSyncProtocol;
use crate::network::Panda;
use crate::node::actor::{NodeActor, ToNodeActor};
use crate::topic::{Publication, Subscription};
use crate::JoinErrToStr;

pub struct Node {
    pub config: Config,
    node_id: PublicKey,
    direct_addresses: Vec<SocketAddr>,
    node_actor_tx: mpsc::Sender<ToNodeActor>,
    actor_handle: Shared<MapErr<AbortOnDropHandle<()>, JoinErrToStr>>,
}

impl Node {
    /// Configure and spawn a node.
    pub async fn spawn(config: Config, private_key: PrivateKey) -> Result<Self> {
        // 1. Connect with NATS client to server and consume streams over "subjects" we're
        //    interested in.
        let nats = Nats::new(config.clone()).await?;

        // 2. Configure rhio network.
        let network_id_hash = Hash::new(config.node.network_id.as_bytes());
        let network_id = network_id_hash.as_bytes();
        let mut network_config = NetworkConfig {
            bind_port: config.node.bind_port,
            network_id: *network_id,
            ..Default::default()
        };

        for node in &config.node.known_nodes {
            network_config.direct_node_addresses.push((
                node.public_key,
                node.direct_addresses.clone(),
                None,
            ));
        }

        let sync_protocol =
            RhioSyncProtocol::new(config.clone(), nats.clone(), private_key.clone());

        let builder = NetworkBuilder::from_config(network_config)
            .private_key(private_key.clone())
            .sync(sync_protocol);

        // 3. Configure and set up blob store and connection handlers for blob replication.
        // @TODO: Will be removed soon.
        let blob_store = BlobsMemoryStore::new();
        let (network, blobs_handler) = BlobsHandler::from_builder(builder, blob_store).await?;
        let blobs = Blobs::new(config.s3.clone(), blobs_handler);

        // 4. Move all networking logic into dedicated "panda" actor, dealing with p2p networking,
        //    data replication and gossipping.
        let node_id = network.node_id();
        let direct_addresses = network
            .direct_addresses()
            .await
            .ok_or_else(|| anyhow!("socket is not bind to any interface"))?;
        let panda = Panda::new(network);

        // 5. Finally spawn actor which orchestrates blob storage and handling, p2panda networking
        //    and NATS JetStream consumers.
        let (node_actor_tx, node_actor_rx) = mpsc::channel(256);
        let node_actor = NodeActor::new(nats, panda, blobs, node_actor_rx);
        let actor_handle = tokio::task::spawn(async move {
            if let Err(err) = node_actor.run().await {
                error!("node actor failed: {err:?}");
            }
        });

        let actor_drop_handle = AbortOnDropHandle::new(actor_handle)
            .map_err(Box::new(|e: JoinError| e.to_string()) as JoinErrToStr)
            .shared();

        let node = Node {
            config,
            node_id,
            direct_addresses,
            node_actor_tx,
            actor_handle: actor_drop_handle,
        };

        Ok(node)
    }

    /// Returns the PublicKey of this node which is used as it's ID.
    ///
    /// This ID is the unique addressing information of this node and other peers must know it to
    /// be able to connect to this node.
    pub fn id(&self) -> PublicKey {
        self.node_id
    }

    /// Returns the direct addresses of this Node.
    ///
    /// The direct addresses of the Node are those that could be used by other nodes to establish
    /// direct connectivity, depending on the network situation. The yielded lists of direct
    /// addresses contain both the locally-bound addresses and the Node's publicly reachable
    /// addresses discovered through mechanisms such as STUN and port mapping. Hence usually only a
    /// subset of these will be applicable to a certain remote node.
    pub fn direct_addresses(&self) -> Vec<SocketAddr> {
        self.direct_addresses.clone()
    }

    pub async fn publish(&self, publication: Publication) -> Result<()> {
        let (reply, reply_rx) = oneshot::channel();
        self.node_actor_tx
            .send(ToNodeActor::Publish { publication, reply })
            .await?;
        reply_rx.await?
    }

    pub async fn subscribe(&self, subscription: Subscription) -> Result<()> {
        let (reply, reply_rx) = oneshot::channel();
        self.node_actor_tx
            .send(ToNodeActor::Subscribe {
                subscription,
                reply,
            })
            .await?;
        reply_rx.await?
    }

    /// Graceful shutdown of the rhio node.
    pub async fn shutdown(self) -> Result<()> {
        let (reply, reply_rx) = oneshot::channel();
        self.node_actor_tx
            .send(ToNodeActor::Shutdown { reply })
            .await?;
        reply_rx.await?;
        self.actor_handle.await.map_err(|err| anyhow!("{err}"))?;
        Ok(())
    }
}
