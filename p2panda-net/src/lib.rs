// SPDX-License-Identifier: AGPL-3.0-or-later

#![allow(unused)]
pub mod config;
pub mod discovery;
mod handshake;
pub mod network;
mod peers;
mod protocols;

#[cfg(feature = "mdns")]
pub use discovery::mdns::LocalDiscovery;
pub use network::{Network, NetworkBuilder, RelayMode};
pub use protocols::ProtocolHandler;

pub type NetworkId = [u8; 32];

pub type TopicId = [u8; 32];
