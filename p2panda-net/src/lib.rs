// SPDX-License-Identifier: AGPL-3.0-or-later

#![allow(unused)]
pub mod discovery;
mod handshake;
pub mod network;
mod protocols;

pub use discovery::MulticastDNSDiscovery;
pub use network::{Network, NetworkBuilder, RelayMode};

pub type NetworkId = [u8; 32];

pub type TopicId = [u8; 32];
