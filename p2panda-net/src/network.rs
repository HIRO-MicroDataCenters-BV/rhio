// SPDX-License-Identifier: AGPL-3.0-or-later

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use futures_lite::StreamExt;
use iroh_gossip::net::{Gossip, GOSSIP_ALPN};
use iroh_gossip::proto::Config as GossipConfig;
use iroh_net::dns::node_info::NodeInfo;
use iroh_net::endpoint::TransportConfig;
use iroh_net::key::SecretKey;
use iroh_net::relay::{RelayMap, RelayNode};
use iroh_net::util::SharedAbortingJoinHandle;
use iroh_net::{Endpoint, NodeAddr, NodeId};
use p2panda_core::{PrivateKey, PublicKey};
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, error_span, warn, Instrument};
use url::Url;

use crate::config::{Config, DEFAULT_BIND_PORT};
use crate::discovery::{Discovery, DiscoveryMap};
use crate::engine::Engine;
use crate::handshake::{Handshake, HANDSHAKE_ALPN};
use crate::protocols::{ProtocolHandler, ProtocolMap};
use crate::{NetworkId, TopicId};

/// Maximum number of streams accepted on a QUIC connection.
const MAX_STREAMS: u32 = 1024;

/// Maximum number of parallel QUIC connections.
const MAX_CONNECTIONS: u32 = 1024;

/// How long we wait at most for some endpoints to be discovered.
const ENDPOINT_WAIT: Duration = Duration::from_secs(5);

#[derive(Debug, PartialEq)]
pub enum RelayMode {
    Disabled,
    Custom(Vec<RelayNode>),
}

// Creates an overlay network for peers grouped under the same "network id". All peers can
// subscribe to multiple "topics" in this overlay and hook into a data stream per topic where
// they'll receive from and send any data to.
pub struct NetworkBuilder {
    bind_port: Option<u16>,
    direct_node_addresses: Vec<NodeAddr>,
    discovery: Option<DiscoveryMap>,
    gossip_config: Option<GossipConfig>,
    network_id: NetworkId,
    protocols: ProtocolMap,
    relay_mode: RelayMode,
    secret_key: Option<SecretKey>,
}

impl NetworkBuilder {
    // Set a network identifier. It'll be used as an identifier for protocol handshake & discovery
    pub fn new(network_id: NetworkId) -> Self {
        Self {
            bind_port: None,
            direct_node_addresses: Vec::new(),
            discovery: None,
            gossip_config: None,
            network_id,
            protocols: Default::default(),
            relay_mode: RelayMode::Disabled,
            secret_key: None,
        }
    }

    // Instantiate a network builder from a network configuration.
    pub fn from_config(config: Config) -> Self {
        let mut network_builder = Self::new(config.network_id).bind_port(config.bind_port);

        for (public_key, addresses) in config.direct_node_addresses {
            network_builder = network_builder.direct_address(public_key, addresses)
        }

        for url in config.relay_addresses {
            // If a port is not given we fallback to 0 which signifies the default stun port
            // should be used.
            let port = url.port().unwrap_or(0);
            network_builder = network_builder.relay(url, false, port)
        }

        network_builder
    }

    pub fn bind_port(mut self, port: u16) -> Self {
        self.bind_port.replace(port);
        self
    }

    // Sets or overwrites the private key, if not set it'll generate a new, random key when
    // building the network.
    pub fn private_key(mut self, private_key: PrivateKey) -> Self {
        self.secret_key = Some(SecretKey::from_bytes(private_key.as_bytes()));
        self
    }

    // adds one or more relay nodes, if not set no relay will be used and only direct connections
    // are possible (you need to provide socket addresses yourself).
    //
    // The relay will help us with STUN (and _not_ rendezvouz as in aquadoggo)
    pub fn relay(mut self, url: Url, stun_only: bool, stun_port: u16) -> Self {
        self.relay_mode = match self.relay_mode {
            RelayMode::Disabled => RelayMode::Custom(vec![RelayNode {
                url: url.into(),
                stun_only,
                stun_port,
            }]),
            RelayMode::Custom(mut list) => RelayMode::Custom({
                list.push(RelayNode {
                    url: url.into(),
                    stun_only,
                    stun_port,
                });
                list
            }),
        };
        self
    }

    // Kinda like a "discovery" technique, but manually
    // * Manual direct addresses (no relay required)
    // * .. or only node id of at least one peer to get into gossip overlay (relay required)
    pub fn direct_address(mut self, node_id: PublicKey, addresses: Vec<SocketAddr>) -> Self {
        let node_id = NodeId::from_bytes(node_id.as_bytes()).expect("invalid public key");
        self.direct_node_addresses
            .push(NodeAddr::new(node_id).with_direct_addresses(addresses));
        self
    }

    // Adds one or more discovery strategy. This can be for example:
    // * mDNS
    // * Rendesvouz / Boostrap Node
    // * ...
    pub fn discovery(mut self, handler: impl Discovery + 'static) -> Self {
        self.discovery = match self.discovery {
            Some(mut list) => {
                list.add(handler);
                Some(list)
            }
            None => Some(DiscoveryMap::from_services(vec![Box::new(handler)])),
        };
        self
    }

    // Gossip mode is always on, maybe we can only configure it here (max active and passive peers
    // etc.) or provide it with a custom implementation?
    pub fn gossip(mut self, config: GossipConfig) -> Self {
        self.gossip_config = Some(config);
        self
    }

    // Add protocols which this node will accept.
    pub fn protocol(
        mut self,
        protocol_name: &'static [u8],
        handler: impl ProtocolHandler + 'static,
    ) -> Self {
        self.protocols.insert(protocol_name, Arc::new(handler));
        self
    }

    pub async fn build(mut self) -> Result<Network> {
        let secret_key = self.secret_key.unwrap_or(SecretKey::generate());

        // Build p2p endpoint and bind the QUIC socket
        let endpoint = {
            let mut transport_config = TransportConfig::default();
            transport_config
                .max_concurrent_bidi_streams(MAX_STREAMS.into())
                .max_concurrent_uni_streams(0u32.into());

            let relay_mode = match self.relay_mode {
                RelayMode::Disabled => iroh_net::relay::RelayMode::Disabled,
                RelayMode::Custom(list) => iroh_net::relay::RelayMode::Custom(
                    RelayMap::from_nodes(list).expect("relay list can not contain duplicates"),
                ),
            };

            Endpoint::builder()
                .transport_config(transport_config)
                .secret_key(secret_key.clone())
                .relay_mode(relay_mode)
                .concurrent_connections(MAX_CONNECTIONS)
                .bind(self.bind_port.unwrap_or(DEFAULT_BIND_PORT))
                .await?
        };

        let node_addr = endpoint.node_addr().await?;

        // Set up gossip overlay handler
        let gossip = Gossip::from_endpoint(
            endpoint.clone(),
            self.gossip_config.unwrap_or_default(),
            &node_addr.info,
        );

        let engine = Engine::new(self.network_id, endpoint.clone(), gossip.clone());
        let handshake = Handshake::new(gossip.clone());

        // Add direct addresses to address book
        for direct_addr in &self.direct_node_addresses {
            engine.add_peer(direct_addr.clone()).await?;
        }

        let inner = Arc::new(NetworkInner {
            cancel_token: CancellationToken::new(),
            discovery: self.discovery,
            endpoint: endpoint.clone(),
            gossip: gossip.clone(),
            network_id: self.network_id,
            engine,
            secret_key,
        });

        // Register core protocols all nodes accept
        self.protocols.insert(GOSSIP_ALPN, Arc::new(gossip));
        self.protocols.insert(HANDSHAKE_ALPN, Arc::new(handshake));
        let protocols = Arc::new(self.protocols);
        if let Err(err) = inner.endpoint.set_alpns(protocols.alpns()) {
            inner.shutdown(protocols).await;
            return Err(err);
        }

        // Create and spawn network task in runtime
        let fut = inner
            .clone()
            .spawn(protocols.clone())
            .instrument(error_span!("node", me=%node_addr.node_id.fmt_short()));
        let task = tokio::task::spawn(fut);

        let network = Network {
            inner,
            task: task.into(),
            protocols,
        };

        // Wait for a single direct address update, to make sure we found at least one direct
        // address
        let wait_for_endpoints = {
            async move {
                tokio::time::timeout(ENDPOINT_WAIT, endpoint.direct_addresses().next())
                    .await
                    .context("waiting for endpoint")?
                    .context("no endpoints given to establish at least one connection")?;
                Ok(())
            }
        };

        if let Err(err) = wait_for_endpoints.await {
            network.shutdown().await.ok();
            return Err(err);
        }

        Ok(network)
    }
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub struct Network {
    inner: Arc<NetworkInner>,
    protocols: Arc<ProtocolMap>,
    task: SharedAbortingJoinHandle<()>,
}

#[allow(dead_code)]
#[derive(Debug)]
struct NetworkInner {
    cancel_token: CancellationToken,
    discovery: Option<DiscoveryMap>,
    endpoint: Endpoint,
    gossip: Gossip,
    network_id: NetworkId,
    engine: Engine,
    secret_key: SecretKey,
}

impl NetworkInner {
    async fn spawn(self: Arc<Self>, protocols: Arc<ProtocolMap>) {
        let (ipv4, ipv6) = self.endpoint.bound_sockets();
        debug!(
            "listening at: {}{}",
            ipv4,
            ipv6.map(|addr| format!(" and {addr}")).unwrap_or_default()
        );

        let mut join_set = JoinSet::<Result<()>>::new();

        // Spawn a task that updates the gossip endpoints and discovery services
        {
            let inner = self.clone();
            join_set.spawn(async move {
                let mut stream = inner.endpoint.direct_addresses();
                while let Some(eps) = stream.next().await {
                    if let Err(err) = inner.gossip.update_direct_addresses(&eps) {
                        warn!("Failed to update direct addresses for gossip: {err:?}");
                    }

                    if let Some(discovery) = &inner.discovery {
                        let relay_url = inner.endpoint.home_relay();
                        let direct_addresses = eps.iter().map(|a| a.addr).collect();
                        let addr = NodeInfo {
                            node_id: inner.endpoint.node_id(),
                            relay_url: relay_url.map(|url| url.into()),
                            direct_addresses,
                        };

                        if let Err(err) = discovery.update_local_address(&addr) {
                            warn!("Failed to update direct addresses for discovery: {err:?}");
                        }
                    }
                }
                warn!("failed to retrieve local endpoints");
                Ok(())
            });
        }

        // Subscribe to all discovery channels where we might find new peers
        let mut discovery = match self.discovery.as_ref() {
            Some(discovery) => discovery.subscribe(self.network_id),
            None => None,
        };

        loop {
            tokio::select! {
                // Do not let tokio select futures randomly but with top-to-bottom priority
                biased;
                // Exit loop when shutdown was signalled somewhere else
                _ = self.cancel_token.cancelled() => {
                    break;
                },
                // Handle incoming p2p connections
                Some(connecting) = self.endpoint.accept() => {
                    let protocols = protocols.clone();
                    join_set.spawn(async move {
                        handle_connection(connecting, protocols).await;
                        Ok(())
                    });
                },
                // Handle discovered peers
                Some(event) = discovery.as_mut().unwrap().next(), if discovery.is_some() => {
                    match event {
                        Ok(event) => {
                            if let Err(err) = self.engine.add_peer(event.node_info.into()).await {
                                error!("Engine failed on add_peer: {err:?}");
                                break;
                            }
                        }
                        Err(err) => {
                            error!("Discovery service failed: {err:?}");
                            break;
                        },
                    }
                },
                // Handle task terminations and quit on panics
                res = join_set.join_next(), if !join_set.is_empty() => {
                    match res {
                        Some(Err(outer)) => {
                            if outer.is_panic() {
                                error!("Task panicked: {outer:?}");
                                break;
                            } else if outer.is_cancelled() {
                                debug!("Task cancelled: {outer:?}");
                            } else {
                                error!("Task failed: {outer:?}");
                                break;
                            }
                        }
                        Some(Ok(Err(inner))) => {
                            debug!("Task errored: {inner:?}");
                        }
                        _ => {}
                    }
                },
                else => break,
            }
        }

        self.shutdown(protocols).await;

        // Abort remaining tasks
        join_set.shutdown().await;
    }

    async fn shutdown(&self, protocols: Arc<ProtocolMap>) {
        // We ignore all errors during shutdown
        let _ = tokio::join!(
            // Close the endpoint. Closing the Endpoint is the equivalent of calling
            // Connection::close on all connections: Operations will immediately fail with
            // ConnectionError::LocallyClosed. All streams are interrupted, this is not graceful
            self.endpoint
                .clone()
                .close(1u32.into(), b"provider terminating"),
            // Shutdown engine
            self.engine.shutdown(),
            // Shutdown protocol handlers
            protocols.shutdown(),
        );
    }
}

impl Network {
    pub fn node_id(&self) -> PublicKey {
        PublicKey::from_bytes(self.inner.endpoint.node_id().as_bytes())
            .expect("public key already checked")
    }

    pub async fn direct_addresses(&self) -> Option<Vec<SocketAddr>> {
        if let Some(addrs) = self.inner.endpoint.direct_addresses().next().await {
            Some(addrs.into_iter().map(|direct| direct.addr).collect())
        } else {
            None
        }
    }

    // Subscribes to a topic and establishes a bi-directional stream from which we can read and
    // write to.
    //
    // Peers subscribed to a topic can be discovered by others via the gossiping overlay ("neighbor
    // up event"). They'll sync data initially (when a sync protocol is given) and then start
    // "live" mode via gossip broadcast
    pub async fn subscribe(
        &self,
        topic: TopicId,
    ) -> Result<(mpsc::Sender<InEvent>, broadcast::Receiver<OutEvent>)> {
        let (in_tx, in_rx) = mpsc::channel::<InEvent>(128);
        let (out_tx, out_rx) = broadcast::channel::<OutEvent>(128);
        self.inner.engine.subscribe(topic, out_tx, in_rx).await?;
        Ok((in_tx, out_rx))
    }

    pub fn endpoint(&self) -> &Endpoint {
        &self.inner.endpoint
    }

    pub async fn known_peers(&self) -> Result<Vec<NodeAddr>> {
        self.inner.engine.known_peers().await
    }

    // Shutdown of the whole network and all subscriptions and connections
    pub async fn shutdown(self) -> Result<()> {
        // Trigger shutdown of the main run task by activating the cancel token
        self.inner.cancel_token.cancel();

        // Wait for the main task to terminate
        self.task.await.map_err(|err| anyhow!(err))?;

        Ok(())
    }
}

#[derive(Clone, Debug)]
pub enum InEvent {
    Message { bytes: Vec<u8> },
}

#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug)]
pub enum OutEvent {
    Ready,
    Message {
        bytes: Vec<u8>,
        delivered_from: PublicKey,
    },
}

async fn handle_connection(
    mut connecting: iroh_net::endpoint::Connecting,
    protocols: Arc<ProtocolMap>,
) {
    let alpn = match connecting.alpn().await {
        Ok(alpn) => alpn,
        Err(err) => {
            warn!("Ignoring connection: invalid handshake: {:?}", err);
            return;
        }
    };
    let Some(handler) = protocols.get(&alpn) else {
        warn!("Ignoring connection: unsupported ALPN protocol");
        return;
    };
    if let Err(err) = handler.accept(connecting).await {
        warn!("Handling incoming connection ended with error: {err}");
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use iroh_net::relay::{RelayNode, RelayUrl};
    use p2panda_core::PrivateKey;
    use url::Url;

    use crate::config::Config;
    use crate::{NetworkBuilder, RelayMode};

    #[tokio::test]
    async fn config() {
        let direct_node_public_key = PrivateKey::new().public_key();
        let relay_address: Url = "https://example.net".parse().unwrap();

        let config = Config {
            bind_port: 2024,
            network_id: [1; 32],
            private_key: Some(PathBuf::new().join("secret-key.txt")),
            direct_node_addresses: vec![(
                direct_node_public_key,
                vec!["0.0.0.0:2026".parse().unwrap()],
            )
                .into()],
            relay_addresses: vec![relay_address.clone()],
        };

        let builder = NetworkBuilder::from_config(config);

        assert_eq!(builder.bind_port, Some(2024));
        assert_eq!(builder.network_id, [1; 32]);
        assert!(builder.secret_key.is_none());
        assert_eq!(builder.direct_node_addresses.len(), 1);
        let relay_node = RelayNode {
            url: RelayUrl::from(relay_address),
            stun_only: false,
            stun_port: 0,
        };
        assert_eq!(builder.relay_mode, RelayMode::Custom(vec![relay_node]));
    }
}
