// SPDX-License-Identifier: AGPL-3.0-or-later

#[cfg(feature = "mdns")]
pub mod mdns;

use std::fmt::Debug;
use std::pin::Pin;

use anyhow::Result;
use futures_lite::stream::Stream;
use iroh_net::dns::node_info::NodeInfo;
use iroh_net::NodeId;

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

    fn update_local_address(&self, addrs: &NodeInfo) -> Result<()> {
        for service in &self.services {
            service.update_local_address(addrs)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct DiscoveryEvent {
    /// Identifier of the discovery service from which this event originated from.
    pub provenance: &'static str,
    pub node_info: NodeInfo,
}

pub trait Discovery: Debug + Send + Sync {
    fn update_local_address(&self, node_info: &NodeInfo) -> Result<()>;

    fn subscribe(&self, network_id: NetworkId) -> Option<BoxedStream<Result<DiscoveryEvent>>> {
        None
    }
}
