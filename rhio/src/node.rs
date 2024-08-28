use std::net::SocketAddr;

use anyhow::{anyhow, Result};
use p2panda_blobs::{Blobs as BlobsHandler, FilesystemStore, MemoryStore as BlobsMemoryStore};
use p2panda_core::{PrivateKey, PublicKey};
use p2panda_net::{Config as NetworkConfig, NetworkBuilder, SharedAbortingJoinHandle};
use tokio::sync::{mpsc, oneshot};
use tracing::error;

use crate::actor::{NodeActor, ToNodeActor};
use crate::blobs::Blobs;
use crate::config::Config;
use crate::nats::Nats;
use crate::panda::Panda;

// @TODO: Give rhio a cool network id
const RHIO_NETWORK_ID: [u8; 32] = [0; 32];

pub struct Node {
    pub config: Config,
    node_id: PublicKey,
    direct_addresses: Vec<SocketAddr>,
    node_actor_tx: mpsc::Sender<ToNodeActor>,
    actor_handle: SharedAbortingJoinHandle<()>,
}

impl Node {
    /// Configure and spawn a node.
    pub async fn spawn(config: Config, private_key: PrivateKey) -> Result<Self> {
        // 1. Configure rhio network
        let mut network_config = NetworkConfig {
            bind_port: config.node.bind_port,
            network_id: RHIO_NETWORK_ID,
            ..Default::default()
        };

        for node in &config.node.known_nodes {
            network_config.direct_node_addresses.push((
                node.public_key,
                node.direct_addresses.clone(),
                None,
            ));
        }

        let builder = NetworkBuilder::from_config(network_config).private_key(private_key.clone());

        // 2. Configure and set up blob store and connection handlers for blob replication
        let (network, blobs) = if let Some(blobs_dir) = &config.blobs_dir {
            // Spawn a rhio actor backed by a filesystem blob store
            let blob_store = FilesystemStore::load(blobs_dir).await?;
            let (network, blobs_handler) = BlobsHandler::from_builder(builder, blob_store).await?;
            let blobs = Blobs::new(config.minio.clone(), blobs_handler);
            (network, blobs)
        } else {
            // Spawn a rhio actor backed by an in memory blob store
            let blob_store = BlobsMemoryStore::new();
            let (network, blobs_handler) = BlobsHandler::from_builder(builder, blob_store).await?;
            let blobs = Blobs::new(config.minio.clone(), blobs_handler);
            (network, blobs)
        };

        // 3. Move all networking logic into dedicated "panda" actor, dealing with p2p networking,
        //    p2panda data replication and gossipping
        let node_id = network.node_id();
        let direct_addresses = network
            .direct_addresses()
            .await
            .ok_or_else(|| anyhow!("socket is not bind to any interface"))?;
        let panda = Panda::new(network);

        // 4. Connect with NATS client to server and consume streams over "subjects" we're
        //    interested in. The NATS jetstream is the p2panda persistance and transport layer and
        //    holds all past and future operations
        let nats = Nats::new(config.clone()).await?;

        // 5. Finally spawn actor which orchestrates blob storage and handling, p2panda networking
        //    and NATS JetStream consumers
        let (node_actor_tx, node_actor_rx) = mpsc::channel(256);
        let node_actor = NodeActor::new(nats, panda, blobs, node_actor_rx);
        let actor_handle = tokio::task::spawn(async move {
            if let Err(err) = node_actor.run().await {
                error!("node actor failed: {err:?}");
            }
        });

        let node = Node {
            config,
            node_id,
            direct_addresses,
            node_actor_tx,
            actor_handle: actor_handle.into(),
        };

        Ok(node)
    }

    /// Returns the PublicKey of this node which is used as it's ID.
    ///
    /// This ID is the unique addressing information of this node and other peers must know it to
    /// be able to connect to this node.
    pub fn id(&self) -> PublicKey {
        self.node_id.clone()
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

    /// Subscribe to a NATS subject over a NATS stream.
    ///
    /// It is possible to subscribe to multiple different subjects over the same stream. Subjects
    /// form "filtered views" on top of streams.
    ///
    /// rhio connects to other clusters in the network and maintains data streams among them per
    /// NATS subject. With the help of p2panda, rhio can maintain streams fully p2p without any
    /// central coordination required. Towards the inner cluster though a central NATS Jetstream
    /// server is used which persists the received data. Jetstream is also used to create new data;
    /// other processes within the cluster publish directly to the NATS Server and rhio will pick
    /// the data up and delegate it further to external clusters.
    ///
    /// Within a cluster we handle everything based on NATS subjects (with their hierarchical
    /// design). Between rhio nodes, using the p2panda protocol, data streams are identified by
    /// p2panda "topics" (non-hierarchical, similar to Kafka topics). While being different in
    /// design and terminology during cross-cluster communication, the general nature of NATS
    /// subjects is preserved from the perspective within each cluster. This is why this high-level
    /// API only refers to NATS "subjects".
    ///
    /// When subscribing to a NATS subject the following steps are taking place:
    ///
    /// 1. An ephemeral, non-acking, push-based Jetstream consumer is created, streaming
    ///    subject-filtered data to rhio.
    /// 2. When creating the subscription, all past data is initially downloaded and moved into an
    ///    in-memory cache. This process might take a while, depending on the number of past
    ///    messages in the stream.
    /// 3. Simultaneously rhio establishes a p2panda gossip overlay with other clusters over that
    ///    topic (hashed subject string) to already receive new data. On receipt it'll be moved
    ///    into the in-memory cache and for persistence published to the NATS server.
    /// 4. When the initial download from the NATS server has finished, we're continuing with
    ///    syncing past state from external clusters using the p2panda sync protocol. Again, this
    ///    data is cached in-memory and persisted on the NATS server.
    /// 5. Finally, when all internal and external cluster data over this "subject" has been
    ///    loaded, we still continue gossiping over future data.
    ///
    /// Since step 2 might take a while (downloading all persisted data from the database) and
    /// blocks all subsequent steps (except entering gossip mode), this method returns an oneshot
    /// receiver the user can await to understand when the initialization has finished.
    pub async fn subscribe(
        &self,
        stream_name: String,
        filter_subject: Option<String>,
    ) -> Result<()> {
        let (reply, reply_rx) = oneshot::channel();
        self.node_actor_tx
            .send(ToNodeActor::Subscribe {
                stream_name,
                filter_subject,
                reply,
            })
            .await?;
        reply_rx.await?
    }

    /// Shutdown the node.
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
