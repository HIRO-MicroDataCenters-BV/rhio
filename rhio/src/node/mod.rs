mod actor;
pub mod config;

use std::net::SocketAddr;

use anyhow::{anyhow, Result};
use futures_util::future::{MapErr, Shared};
use futures_util::{FutureExt, TryFutureExt};
use p2panda_blobs::Blobs as BlobsHandler;
use p2panda_core::{Hash, PrivateKey, PublicKey};
use p2panda_net::{Config as NetworkConfig, NetworkBuilder};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinError;
use tokio_util::task::AbortOnDropHandle;
use tracing::error;

use crate::blobs::watcher::S3Watcher;
use crate::blobs::{blobs_config, store_from_config, Blobs};
use crate::config::Config;
use crate::nats::Nats;
use crate::network::sync::RhioSyncProtocol;
use crate::network::Panda;
use crate::node::actor::{NodeActor, ToNodeActor};
use crate::node::config::NodeConfig;
use crate::topic::{Publication, Subscription};
use crate::JoinErrToStr;

pub struct Node {
    node_id: PublicKey,
    direct_addresses: Vec<SocketAddr>,
    node_actor_tx: mpsc::Sender<ToNodeActor>,
    actor_handle: Shared<MapErr<AbortOnDropHandle<()>, JoinErrToStr>>,
}

impl Node {
    /// Configure and spawn a node.
    pub async fn spawn(config: Config, private_key: PrivateKey) -> Result<Self> {
        // 1. Connect to NATS server and consume streams filtered by NATS subjects.
        let nats = Nats::new(config.clone()).await?;

        // 2. Configure rhio peer-to-peer network.
        let node_config = NodeConfig::new();
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

        let blob_store = store_from_config(&config).await?;

        let sync_protocol = RhioSyncProtocol::new(
            node_config.clone(),
            nats.clone(),
            blob_store.clone(),
            private_key.clone(),
        );

        let builder = NetworkBuilder::from_config(network_config)
            .private_key(private_key.clone())
            .sync(sync_protocol, true);

        // 3. Configure and set up blob store and connection handlers for blob replication.
        let (network, blobs_handler) =
            BlobsHandler::from_builder_with_config(builder, blob_store.clone(), blobs_config())
                .await?;
        let blobs = Blobs::new(blob_store.clone(), blobs_handler);

        // 4. Start a service which watches the S3 buckets for changes.
        let (watcher_tx, watcher_rx) = mpsc::channel(256);
        let watcher = S3Watcher::new(blob_store, watcher_tx);

        // 5. Move all networking logic into dedicated "p2panda" actor, dealing with p2p
        //    networking, data replication and gossipping.
        let node_id = network.node_id();
        let direct_addresses = network
            .direct_addresses()
            .await
            .ok_or_else(|| anyhow!("socket is not bind to any interface"))?;
        let panda = Panda::new(network);

        // 6. Finally spawn actor which orchestrates the "business logic", with the help of the
        //    blob store, p2panda network and NATS JetStream consumers.
        let (node_actor_tx, node_actor_rx) = mpsc::channel(256);
        let node_actor = NodeActor::new(
            node_config,
            private_key,
            nats,
            panda,
            blobs,
            watcher,
            node_actor_rx,
            watcher_rx,
        );
        let actor_handle = tokio::task::spawn(async move {
            if let Err(err) = node_actor.run().await {
                error!("node actor failed: {err:?}");
            }
        });

        let actor_drop_handle = AbortOnDropHandle::new(actor_handle)
            .map_err(Box::new(|e: JoinError| e.to_string()) as JoinErrToStr)
            .shared();

        let node = Node {
            node_id,
            direct_addresses,
            node_actor_tx,
            actor_handle: actor_drop_handle,
        };

        Ok(node)
    }

    /// Returns the public key of this node which is used as it's ID.
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

    /// Inform our node that we want to publish certain data.
    pub async fn publish(&self, publication: Publication) -> Result<()> {
        let (reply, reply_rx) = oneshot::channel();
        self.node_actor_tx
            .send(ToNodeActor::Publish { publication, reply })
            .await?;
        reply_rx.await?
    }

    /// Inform our node that we're interested in certain data from remote peers.
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
