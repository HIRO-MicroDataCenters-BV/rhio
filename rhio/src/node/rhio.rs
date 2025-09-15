use std::net::SocketAddr;

use anyhow::{Context, Result, anyhow};
use futures_util::future::{MapErr, Shared};
use futures_util::{FutureExt, TryFutureExt};
use p2panda_core::{Hash, PrivateKey, PublicKey};
use p2panda_net::{Config as NetworkConfig, NodeAddress};
use s3::error::S3Error;
use tokio::sync::mpsc::Receiver;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinError;
use tokio_util::task::AbortOnDropHandle;
use tracing::{error, warn};

use crate::JoinErrToStr;
use crate::blobs::Blobs;
use crate::blobs::watcher::S3Event;
use crate::nats::Nats;
use crate::network::Panda;
use crate::node::actor::{NodeActor, ToNodeActor};
use crate::node::config::NodeConfig;
use crate::topic::{Publication, Subscription};
use rhio_config::configuration::Config;

#[derive(Debug, Clone, Default)]
pub struct NodeOptions {
    pub public_key: PublicKey,
    pub private_key: PrivateKey,
    pub direct_addresses: Vec<SocketAddr>,
    pub node_config: NodeConfig,
}

pub struct Node {
    node_id: PublicKey,
    direct_addresses: Vec<SocketAddr>,
    node_actor_tx: mpsc::Sender<ToNodeActor>,
    actor_handle: Shared<MapErr<AbortOnDropHandle<()>, JoinErrToStr>>,
}

impl Node {
    /// Create a new Rhio node.
    pub async fn new(
        nats: Nats,
        blobs: Option<Blobs>,
        watcher_rx: Receiver<Result<S3Event, S3Error>>,
        panda: Panda,
        options: NodeOptions,
    ) -> Result<Self> {
        // 6. Finally spawn actor which orchestrates the "business logic", with the help of the
        //    blob store, p2panda network and NATS JetStream consumers.
        let (node_actor_tx, node_actor_rx) = mpsc::channel(512);
        let node_actor = NodeActor::new(
            options.node_config,
            options.private_key,
            nats,
            panda,
            blobs,
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
            node_id: options.public_key,
            direct_addresses: options.direct_addresses,
            node_actor_tx,
            actor_handle: actor_drop_handle,
        };

        Ok(node)
    }

    pub async fn configure_p2p_network(
        config: &Config,
    ) -> Result<(NodeConfig, NetworkConfig), anyhow::Error> {
        let node_config = NodeConfig::new();
        let network_id_hash = Hash::new(config.node.network_id.as_bytes());
        let network_id = network_id_hash.as_bytes();
        let mut network_config = NetworkConfig {
            bind_port_v4: config.node.bind_port,
            network_id: *network_id,
            ..Default::default()
        };
        for node in &config.node.known_nodes {
            // Resolve FQDN strings into IP addresses.
            let mut direct_addresses = Vec::new();
            for addr in &node.direct_addresses {
                let maybe_peers = tokio::net::lookup_host(addr)
                    .await
                    .context(format!("Unable to lookup host {:?}, skipping", addr));
                match maybe_peers {
                    Ok(peers) => {
                        for resolved in peers {
                            direct_addresses.push(resolved);
                        }
                    }
                    Err(err) => {
                        warn!("{:?}", err);
                    }
                }
            }
            let node_address = NodeAddress {
                public_key: node.public_key,
                direct_addresses,
                relay_url: None,
            };
            network_config.direct_node_addresses.push(node_address);
        }
        Ok((node_config, network_config))
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
