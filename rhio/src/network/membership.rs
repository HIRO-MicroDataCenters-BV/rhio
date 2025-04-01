use std::{collections::HashMap, time::Duration};

use anyhow::Result;
use iroh_base::{key::NodeId, node_addr::NodeAddr};
use loole::Receiver;
use p2panda_core::PublicKey;
use p2panda_discovery::DiscoveryEvent;
use p2panda_discovery::{BoxedStream, Discovery};
use rhio_config::configuration::{DiscoveryOptions, KnownNode};
use tokio_util::task::AbortOnDropHandle;
use tracing::trace;

type SubscribeReceiver = Receiver<Result<DiscoveryEvent>>;

struct NodeInfo {
    pub addresses: Vec<String>,
    pub addr: Option<NodeAddr>,
}

/// This is a simple implementation of peer discovery built specifically to workaround cases
/// where peers may not be running or being redeployed in kubernetes.
/// In those cases the DNS will not be able to resolve IP addresses of peers,
/// because kubernetes services will not be available and therefore FQDN records will not exist.
///
/// # Responsibilities
/// - Maintains a list of known peers and their associated information, such as
///   addresses and resolved `NodeAddr`.
/// - Periodically performs peer discovery by resolving addresses of known peers
///   and updating their `NodeAddr` if changes are detected.
/// - Sends discovery events to subscribers when new peers are discovered.
///
#[derive(Debug)]
pub struct Membership {
    #[allow(dead_code)]
    handle: AbortOnDropHandle<()>,
    rx: SubscribeReceiver,
}

impl Membership {
    pub fn new(known_nodes: &Vec<KnownNode>, options: DiscoveryOptions) -> Self {
        let (tx, rx) = loole::bounded(64);
        let mut known_peers: HashMap<PublicKey, NodeInfo> = HashMap::new();
        for node in known_nodes {
            let node_info = NodeInfo {
                addr: None,
                addresses: node.direct_addresses.clone(),
            };
            known_peers.insert(node.public_key, node_info);
        }

        let sender = tx.clone();

        let handle = tokio::task::spawn(async move {
            let query_interval = Duration::from_secs(options.query_interval_seconds);
            let mut interval = tokio::time::interval(query_interval);

            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        let discovery_result = Membership::discover_peers(&mut known_peers).await;
                        trace!("Peer discovery result: {:?}", discovery_result);
                        for node_addr in discovery_result {
                            sender.send(Ok(DiscoveryEvent{ provenance: "peer_discovery", node_addr })).ok();
                        }
                    },
                }
            }
        });

        Self {
            handle: AbortOnDropHandle::new(handle),
            rx,
        }
    }

    async fn discover_peers(known_peers: &mut HashMap<PublicKey, NodeInfo>) -> Vec<NodeAddr> {
        let mut discovered = vec![];
        for (public_key, info) in known_peers.iter_mut() {
            let mut direct_addresses = vec![];
            for fqdn in &info.addresses {
                let maybe_peers = tokio::net::lookup_host(fqdn).await;
                if let Ok(peers) = maybe_peers {
                    for resolved in peers {
                        direct_addresses.push(resolved);
                    }
                }
            }
            if direct_addresses.is_empty() {
                continue;
            }
            let key = NodeId::from_bytes(public_key.as_bytes()).expect("invalid public key");
            let node_addr = Some(NodeAddr::from_parts(key, None, direct_addresses));
            if node_addr != info.addr {
                info.addr = node_addr.clone();
                if let Some(addr) = node_addr {
                    discovered.push(addr);
                }
            }
        }
        discovered
    }
}

impl Discovery for Membership {
    fn update_local_address(&self, _node_addr: &NodeAddr) -> Result<()> {
        Ok(())
    }

    fn subscribe(&self, _network_id: [u8; 32]) -> Option<BoxedStream<Result<DiscoveryEvent>>> {
        let rx_stream = Box::pin(self.rx.clone().into_stream());
        Some(rx_stream)
    }
}
