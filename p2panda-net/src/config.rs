use std::net::SocketAddr;

use iroh_net::{NodeAddr as IrohNodeAddr, NodeId};
use p2panda_core::{PrivateKey, PublicKey};
use serde::{Deserialize, Serialize};
use url::Url;

/// Default network key.
pub const DEFAULT_NETWORK_KEY: [u8; 32] = [0; 32];

/// Default port of a node socket.
pub const DEFAULT_BIND_PORT: u16 = 2022;

/// Default configuration for local discovery.
#[cfg(feature = "mdns")]
pub const DEFAULT_LOCAL_DISCOVERY: bool = true;

#[cfg(not(feature = "mdns"))]
pub const DEFAULT_LOCAL_DISCOVERY: bool = false;

pub type NodeAddr = (PublicKey, Vec<SocketAddr>);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Config {
    pub bind_port: u16,
    pub network_key: [u8; 32],
    pub private_key: Option<PrivateKey>,
    pub local_discovery: bool,
    pub direct_node_addresses: Vec<NodeAddr>,
    pub relay_addresses: Vec<Url>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            network_key: DEFAULT_NETWORK_KEY,
            bind_port: DEFAULT_BIND_PORT,
            local_discovery: DEFAULT_LOCAL_DISCOVERY,
            private_key: None,
            direct_node_addresses: vec![],
            relay_addresses: vec![],
        }
    }
}

pub fn to_node_addr(public_key: &PublicKey, addresses: &[SocketAddr]) -> IrohNodeAddr {
    let node_id = NodeId::from_bytes(public_key.as_bytes()).expect("invalid public key");
    IrohNodeAddr::new(node_id).with_direct_addresses(addresses.to_vec())
}
