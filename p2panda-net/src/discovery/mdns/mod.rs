// SPDX-License-Identifier: AGPL-3.0-or-later

mod dns;
mod socket;

use std::collections::HashMap;
use std::str::FromStr;
use std::time::Duration;

use anyhow::Result;
use flume::Sender;
use futures_lite::StreamExt;
use hickory_proto::rr::Name;
use iroh_base::base32;
use iroh_net::dns::node_info::NodeInfo;
use iroh_net::util::AbortingJoinHandle;
use iroh_net::NodeId;

use crate::discovery::mdns::dns::{make_query, make_response, parse_message, MulticastDNSMessage};
use crate::discovery::mdns::socket::{send, socket_v4};
use crate::discovery::{BoxedStream, Discovery, DiscoveryEvent};
use crate::NetworkId;

const MDNS_PROVENANCE: &str = "mdns";
const MDNS_QUERY_INTERVAL: Duration = Duration::from_secs(5);

pub type ServiceName = Name;

type SubscribeSender = Sender<Result<DiscoveryEvent>>;

enum Message {
    Subscribe(ServiceName, SubscribeSender),
    UpdateLocalAddress(NodeInfo),
}

#[derive(Debug)]
pub struct LocalDiscovery {
    handle: AbortingJoinHandle<()>,
    tx: Sender<Message>,
}

impl LocalDiscovery {
    pub fn new() -> Result<Self> {
        let (tx, rx) = flume::bounded(64);
        let tx_clone = tx.clone();

        let socket = socket_v4()?;

        let mut subscribers: HashMap<ServiceName, Vec<SubscribeSender>> = HashMap::new();
        let mut my_node_info: Option<NodeInfo> = None;

        let handle = tokio::task::spawn(async move {
            let mut interval = tokio::time::interval(MDNS_QUERY_INTERVAL);
            let mut buf = [0; 1472];

            loop {
                tokio::select! {
                    biased;
                    Ok((len, addr)) = socket.recv_from(&mut buf) => {
                        let Some(msg) = parse_message(&buf[..len], addr.ip()) else {
                            continue;
                        };

                        match msg {
                            MulticastDNSMessage::Query(service_name) => {
                                let Some(my_node_info) = &my_node_info else {
                                    continue;
                                };

                                if subscribers.contains_key(&service_name) {
                                    let response = make_response(&service_name, my_node_info);
                                    send(&socket, response).await;
                                }
                            },
                            MulticastDNSMessage::Response(service_name, node_infos) => {
                                let Some(my_node_info) = &my_node_info else {
                                    continue;
                                };

                                let Some(subscribers) = subscribers.get(&service_name) else {
                                    continue;
                                };

                                for subscribe_tx in subscribers {
                                    for node_info in &node_infos {
                                        if node_info.node_id == my_node_info.node_id {
                                            continue;
                                        }

                                        subscribe_tx
                                            .send_async(Ok(DiscoveryEvent {
                                                provenance: MDNS_PROVENANCE,
                                                node_info: node_info.clone(),
                                            }))
                                            .await
                                            .ok();
                                    }
                                }
                            }
                        }
                    },
                    _ = interval.tick() => {
                        for service_name in subscribers.keys() {
                            send(&socket, make_query(service_name)).await;
                        }
                    },
                    Ok(msg) = rx.recv_async() => {
                        match msg {
                            Message::Subscribe(service_name, subscribe_tx) => {
                                if let Some(subscriber) = subscribers.get_mut(&service_name) {
                                    subscriber.push(subscribe_tx);
                                } else {
                                    subscribers.insert(service_name, vec![subscribe_tx]);
                                }
                            }
                            Message::UpdateLocalAddress(ref info) => {
                                my_node_info = Some(info.clone());
                            }
                        }
                    },
                    else => break,
                }
            }
        });

        Ok(Self {
            handle: handle.into(),
            tx,
        })
    }
}

impl Discovery for LocalDiscovery {
    fn subscribe(&self, network_id: NetworkId) -> Option<BoxedStream<Result<DiscoveryEvent>>> {
        let (subscribe_tx, subscribe_rx) = flume::bounded(16);
        let service_tx = self.tx.clone();
        let service_name =
            Name::from_str(&format!("_{}._udp.local.", base32::fmt(network_id))).unwrap();

        tokio::spawn(async move {
            service_tx
                .send_async(Message::Subscribe(service_name, subscribe_tx))
                .await
                .ok();
        });

        Some(subscribe_rx.into_stream().boxed())
    }

    fn update_local_address(&self, info: &NodeInfo) -> Result<()> {
        let tx = self.tx.clone();
        let info = info.clone();
        tokio::spawn(async move {
            tx.send_async(Message::UpdateLocalAddress(info)).await.ok();
        });
        Ok(())
    }
}
