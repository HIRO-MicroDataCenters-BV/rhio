// SPDX-License-Identifier: AGPL-3.0-or-later

#[cfg(feature = "mdns")]
pub mod mdns;

use std::fmt::{Debug, Display};
use std::pin::Pin;

use anyhow::Result;
use futures_lite::stream::Stream;
use iroh_base::base32;
use iroh_net::dns::node_info::NodeInfo;
use iroh_net::endpoint::DirectAddr;
use iroh_net::{AddrInfo, Endpoint, NodeAddr, NodeId};

use crate::NetworkId;

pub type BoxedStream<T> = Pin<Box<dyn Stream<Item = T> + Send + 'static>>;

#[derive(Debug, Default)]
pub(crate) struct DiscoveryMap {
    services: Vec<Box<dyn Discovery>>,
}

impl DiscoveryMap {
    pub fn from_services(services: Vec<Box<dyn Discovery>>) -> Self {
        Self { services }
    }

    pub fn add(&mut self, service: impl Discovery + 'static) {
        self.services.push(Box::new(service));
    }
}

impl Discovery for DiscoveryMap {
    fn subscribe(&self, network_id: NetworkId) -> Option<BoxedStream<Result<DiscoveryEvent>>> {
        let streams = self
            .services
            .iter()
            .filter_map(|service| service.subscribe(network_id));
        let streams = futures_buffered::Merge::from_iter(streams);
        Some(Box::pin(streams))
    }

    fn update_local_address(&self, addrs: &DiscoveryNodeInfo) -> Result<()> {
        for service in &self.services {
            service.update_local_address(addrs)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct SecretNodeInfo(Vec<u8>);

impl SecretNodeInfo {
    pub fn new(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }

    pub fn encrypt(node_info: NodeInfo, secret: String) -> Self {
        Self(Vec::new())
    }

    pub fn decrypt(secret: String) -> AddrInfo {
        AddrInfo::default()
    }
}

impl Display for SecretNodeInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&base32::fmt(self.0.clone()))
    }
}

#[derive(Debug, Clone)]
pub enum DiscoveryNodeInfo {
    NodeInfo(NodeInfo),

    /// Encrypted info required to establish connection with discovered peer. Together with the
    /// shared secret this info can be decrypted and finally used to establish the connection.
    Secret(SecretNodeInfo),
}

#[derive(Debug, Clone)]
pub struct DiscoveryEvent {
    /// Identifier of the discovery service from which this event originated from.
    pub provenance: &'static str,
    pub node_info: DiscoveryNodeInfo,
}

pub trait Discovery: Debug + Send + Sync {
    fn update_local_address(&self, node_info: &DiscoveryNodeInfo) -> Result<()>;

    fn subscribe(&self, network_id: NetworkId) -> Option<BoxedStream<Result<DiscoveryEvent>>> {
        None
    }
}
