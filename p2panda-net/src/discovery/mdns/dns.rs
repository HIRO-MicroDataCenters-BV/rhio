// SPDX-License-Identifier: AGPL-3.0-or-later

use std::borrow::Cow;
use std::net::IpAddr;

use hickory_proto::op::{Message, MessageType, Query};
use hickory_proto::rr::{rdata, DNSClass, Name, RData, Record, RecordType};
use hickory_proto::serialize::binary::BinEncodable;
use tracing::{debug, trace};

use crate::discovery::mdns::{PeerId, ServiceName};
use crate::discovery::{DiscoveryNodeInfo, SecretNodeInfo};

pub enum MulticastDNSMessage {
    QueryV4(ServiceName),
    QueryV6(ServiceName),
    Response(ServiceName, PeerId, DiscoveryNodeInfo),
}

pub fn make_query(service_name: &Name) -> Message {
    let mut msg = Message::new();
    msg.set_message_type(MessageType::Query);
    let mut query = Query::new();
    query.set_query_class(DNSClass::IN);
    query.set_query_type(RecordType::PTR);
    query.set_name(service_name.clone());
    msg.add_query(query);
    msg
}

pub fn make_response(
    peer_id: &PeerId,
    service_name: &ServiceName,
    addr_info: &DiscoveryNodeInfo,
) -> Message {
    let mut msg = Message::new();
    msg.set_message_type(MessageType::Response);
    msg.set_authoritative(true);

    let my_srv_name = Name::from_utf8(peer_id)
        .expect("was checked already")
        .clone()
        .append_domain(service_name)
        .expect("was checked already");

    match addr_info {
        // @TODO
        DiscoveryNodeInfo::NodeInfo(_) => todo!(),
        DiscoveryNodeInfo::Secret(ciphertext) => {
            msg.add_answer(Record::from_rdata(
                my_srv_name.to_owned(),
                0,
                RData::TXT(rdata::TXT::new(vec![ciphertext.to_string()])),
            ));
        }
    }

    msg
}

pub fn parse_message(bytes: &[u8], addr: IpAddr) -> Option<MulticastDNSMessage> {
    let packet = match Message::from_vec(bytes) {
        Ok(packet) => packet,
        Err(err) => {
            debug!("error parsing mdns packet: {}", err);
            return None;
        }
    };

    // Handle query
    for question in packet.queries() {
        if question.query_class() != DNSClass::IN {
            trace!(
                "received mdns query with wrong class {}",
                question.query_class()
            );
            continue;
        }
        if question.query_type() != RecordType::PTR {
            trace!(
                "received mDNS query with wrong type {}",
                question.query_type()
            );
            continue;
        }

        let service_name = question.name();

        trace!("received mDNS query for {}", question.name());
        return Some(match addr {
            IpAddr::V4(_) => MulticastDNSMessage::QueryV4(service_name.clone()),
            IpAddr::V6(_) => MulticastDNSMessage::QueryV6(service_name.clone()),
        });
    }

    // Handle responses
    if packet.answers().len() != 1 {
        debug!("received mdns response with too many answers");
        return None;
    }
    let response = &packet.answers()[0];

    if response.dns_class() != DNSClass::IN {
        trace!(
            "received mdns response with wrong class {:?}",
            response.dns_class()
        );
        return None;
    }

    let name = response.name();
    let service_name = name.base_name();

    let Some(peer_id_bytes) = name.iter().next() else {
        return None;
    };
    let Cow::Borrowed(peer_id) = String::from_utf8_lossy(peer_id_bytes) else {
        tracing::debug!(
            "received mDNS response with invalid peer ID {:?}",
            peer_id_bytes
        );
        return None;
    };

    tracing::debug!("received mDNS response for {}", service_name);
    let Some(RData::TXT(txt)) = response.data() else {
        trace!(
            "received mdns response with wrong data {:?}",
            response.data()
        );
        return None;
    };

    let Ok(addr_info_bytes) = txt.to_bytes() else {
        trace!(
            "received mdns with response with invalid bytes in txt {:?}",
            response.data()
        );
        return None;
    };

    // @TODO: Also handle unencrypted addr info
    let addr_info = DiscoveryNodeInfo::Secret(SecretNodeInfo::new(addr_info_bytes));

    Some(MulticastDNSMessage::Response(
        service_name.clone(),
        peer_id.to_owned(),
        addr_info,
    ))
}
