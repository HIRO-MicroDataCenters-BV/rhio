// SPDX-License-Identifier: AGPL-3.0-or-later

use anyhow::Result;
use futures_lite::stream::Boxed as BoxedStream;
use iroh_net::discovery::{Discovery, DiscoveryItem};
use iroh_net::{AddrInfo, Endpoint, NodeId};

use crate::NetworkId;

#[derive(Debug)]
pub struct MulticastDNSDiscovery {
    network_id: NetworkId,
}

impl MulticastDNSDiscovery {
    pub fn new(network_id: NetworkId) -> Self {
        Self { network_id }
    }
}

impl Discovery for MulticastDNSDiscovery {
    fn publish(&self, info: &AddrInfo) {}

    fn resolve(
        &self,
        endpoint: Endpoint,
        node_id: NodeId,
    ) -> Option<BoxedStream<Result<DiscoveryItem>>> {
        None
    }
}
