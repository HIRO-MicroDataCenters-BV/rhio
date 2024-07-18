// SPDX-License-Identifier: AGPL-3.0-or-later

use std::net::SocketAddr;
use std::path::PathBuf;

use iroh_net::{NodeAddr as IrohNodeAddr, NodeId};
use p2panda_core::PublicKey;
use serde::{Deserialize, Serialize};
use url::Url;

use crate::NetworkId;

/// Default network id.
pub const DEFAULT_NETWORK_ID: NetworkId = [0; 32];

/// Default port of a node socket.
pub const DEFAULT_BIND_PORT: u16 = 2022;

/// The default STUN port used by the relay server.
///
/// The STUN port as defined by [RFC 8489](<https://www.rfc-editor.org/rfc/rfc8489#section-18.6>)
pub const DEFAULT_STUN_PORT: u16 = 3478;

pub type NodeAddr = (PublicKey, Vec<SocketAddr>);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Config {
    pub bind_port: u16,
    pub network_id: NetworkId,
    pub private_key: Option<PathBuf>,
    pub direct_node_addresses: Vec<NodeAddr>,
    pub relay_addresses: Vec<Url>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            network_id: DEFAULT_NETWORK_ID,
            bind_port: DEFAULT_BIND_PORT,
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
