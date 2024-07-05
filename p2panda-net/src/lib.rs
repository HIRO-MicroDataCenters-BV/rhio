#![allow(unused)]
use std::net::ToSocketAddrs;

use anyhow::Result;
use iroh_gossip::proto::Config as GossipConfig;
use iroh_net::discovery::Discovery;
use p2panda_core::PrivateKey;
use url::Url;

type NetworkId = [u8; 32];

type TopicId = [u8; 32];

// Creates an overlay network for peers grouped under the same "network id". All peers can
// subscribe to multiple "topics" in this overlay and hook into a data stream per topic where
// they'll receive from and send any data to.
struct NetworkBuilder {}

impl NetworkBuilder {
    // Set a network identifier. It'll be used for:
    // * Transport encryption (?) aka "network key"
    // * Node discovery (?)
    pub fn new(id: NetworkId) -> Self {
        Self {}
    }

    // Sets or overwrites the private key, if not set it'll generate a new, random key when
    // building the network.
    pub fn private_key(self, private_key: PrivateKey) -> Self {
        self
    }

    // Adds one or more relay nodes, if not set no relay will be used and only direct connections
    // are possible (you need to provide socket addresses yourself).
    //
    // The relay will help us with STUN (and _not_ rendezvouz as in aquadoggo)
    pub fn relay(self, url: Url) -> Self {
        self
    }

    // Adds one or more discovery strategy. This can be for example:
    // * Manual direct addresses (required when not using a relay)
    // * mDNS
    // * Rendesvouz / Boostrap Node
    // * DNS + pkarr Nodes
    // * ...
    pub fn discovery(self, handler: impl Discovery) -> Self {
        self
    }

    // Sets or overwrites a handler for a syncing strategy. If not set no initial syncing will be
    // started on connection and the node directly jumps to gossip / "live" mode
    pub fn sync(self, handler: impl Syncing) -> Self {
        self
    }

    // Gossip mode is always on, maybe we can only configure it here (max active and passive peers
    // etc.) or provide it with a custom implementation?
    pub fn gossip(self, config: GossipConfig) -> Self {
        self
    }

    pub fn build(self) -> Network {
        Network {}
    }
}

struct Network {}

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

// Mock `p2panda-core` crate
mod p2panda_core {
    pub struct PrivateKey;
}
