// SPDX-License-Identifier: AGPL-3.0-or-later

pub mod config;
pub mod discovery;
mod engine;
mod handshake;
pub mod network;
mod protocols;
mod message;

#[cfg(feature = "mdns")]
pub use discovery::mdns::LocalDiscovery;
pub use network::{Network, NetworkBuilder, RelayMode};
pub use protocols::ProtocolHandler;
pub use message::{ToBytes, FromBytes};

pub type NetworkId = [u8; 32];

pub type TopicId = [u8; 32];
