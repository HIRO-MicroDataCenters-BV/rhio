#![allow(unused)]
use std::net::{SocketAddr, ToSocketAddrs};

use anyhow::Result;
use iroh_gossip::proto::Config as GossipConfig;
use iroh_net::discovery::{ConcurrentDiscovery, Discovery};
use iroh_net::endpoint::TransportConfig;
use iroh_net::key::SecretKey;
use iroh_net::relay::{RelayMap, RelayNode, RelayUrl};
use iroh_net::{Endpoint, NodeAddr, NodeId};
use p2panda_core::{PrivateKey, PublicKey};
use url::Url;

const DEFAULT_BIND_PORT: u16 = 2022;
const MAX_STREAMS: u32 = 1024;
const MAX_CONNECTIONS: u32 = 1024;

type NetworkId = [u8; 32];

type TopicId = [u8; 32];

// Creates an overlay network for peers grouped under the same "network id". All peers can
// subscribe to multiple "topics" in this overlay and hook into a data stream per topic where
// they'll receive from and send any data to.
struct NetworkBuilder {
    network_id: NetworkId,
    bind_port: Option<u16>,
    discovery: Option<ConcurrentDiscovery>,
    direct_node_addresses: Vec<NodeAddr>,
    gossip_config: Option<GossipConfig>,
    secret_key: Option<SecretKey>,
    relay_mode: RelayMode,
}

impl NetworkBuilder {
    // Set a network identifier. It'll be used as an identifier for protocol handshake & discovery
    pub fn new(network_id: NetworkId) -> Self {
        Self {
            network_id,
            bind_port: None,
            discovery: None,
            direct_node_addresses: Vec::new(),
            gossip_config: None,
            relay_mode: RelayMode::Disabled,
            secret_key: None,
        }
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
    // * DNS + pkarr Nodes
    // * ...
    pub fn discovery(mut self, handler: impl Discovery + 'static) -> Self {
        self.discovery = match self.discovery {
            Some(mut list) => {
                list.add(handler);
                Some(list)
            }
            None => Some(ConcurrentDiscovery::from_services(vec![Box::new(handler)])),
        };
        self
    }

    // Sets or overwrites a handler for a syncing strategy. If not set no initial syncing will be
    // started on connection and the node directly jumps to gossip / "live" mode
    pub fn sync(mut self, handler: impl Syncing) -> Self {
        unimplemented!();
    }

    // Gossip mode is always on, maybe we can only configure it here (max active and passive peers
    // etc.) or provide it with a custom implementation?
    pub fn gossip(mut self, config: GossipConfig) -> Self {
        self.gossip_config = Some(config);
        self
    }

    pub async fn build(self) -> Result<Network> {
        let mut transport_config = TransportConfig::default();
        transport_config
            .max_concurrent_bidi_streams(MAX_STREAMS.into())
            .max_concurrent_uni_streams(0u32.into());

        let secret_key = self.secret_key.unwrap_or(SecretKey::generate());

        let relay_mode = match self.relay_mode {
            RelayMode::Disabled => iroh_net::relay::RelayMode::Disabled,
            RelayMode::Custom(list) => iroh_net::relay::RelayMode::Custom(
                RelayMap::from_nodes(list).expect("relay list can not contain duplicates"),
            ),
        };

        let builder = Endpoint::builder()
            .transport_config(transport_config)
            .secret_key(secret_key.clone())
            .relay_mode(relay_mode)
            .concurrent_connections(MAX_CONNECTIONS);

        let bind_port = self.bind_port.unwrap_or(DEFAULT_BIND_PORT);
        let endpoint = builder.bind(bind_port).await?;

        for node_addr in &self.direct_node_addresses {
            endpoint.add_node_addr(node_addr.clone())?;
        }

        Ok(Network {
            secret_key,
            endpoint,
        })
    }
}

struct Network {
    secret_key: SecretKey,
    endpoint: Endpoint,
}

impl Network {
    // Subscribes to a topic and establishes a bi-directional stream from which we can read and
    // write to.
    //
    // Peers subscribed to a topic can be discovered by others via the gossiping overlay ("neighbor
    // up event"). They'll sync data initially (when a sync protocol is given) and then start
    // "live" mode via gossip broadcast
    pub fn subscribe(&self, id: TopicId) -> (Sender, Receiver) {
        (Sender {}, Receiver {})
    }

    // Shutdown of the whole network and all subscriptions and connections
    pub fn close(&mut self) -> Result<()> {
        Ok(())
    }
}

// Sink to write data into a channel, scoped by a "topic id".
//
// Since this networking layer is independent of any persistance, users of this API will need to
// persist new data they've created themselves and make the sync layer aware of this database
struct Sender {}

// Stream to read data from a channel, scoped by a "topic id". This is commonly used by any given
// sync protocol and gossiping layer, everything will "land" here, independent of where it came
// from and in what way, the only guarantee here is that it came from our overlay network scoped by
// "network id" and "topic id".
//
// Since this networking layer is independent of any persistance users of this API will need to
// persist incoming data themselves and make the sync layer aware of this database
struct Receiver {}

// @TODO
trait Syncing {}

enum RelayMode {
    Disabled,
    Custom(Vec<RelayNode>),
}
