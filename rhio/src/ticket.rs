use std::net::SocketAddr;
use std::{fmt::Display, str::FromStr};

use anyhow::Context;
use p2panda_core::PublicKey;
use p2panda_net::{NodeAddress, RelayUrl};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Ticket(NodeAddress);

impl Ticket {
    pub fn new(public_key: PublicKey, addrs: Vec<SocketAddr>, relay_url: Option<RelayUrl>) -> Self {
        Self((public_key, addrs, relay_url))
    }
}

impl Display for Ticket {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut bytes = Vec::new();
        ciborium::ser::into_writer(&self, &mut bytes).unwrap();
        f.write_str(&hex::encode(&bytes))
    }
}

impl FromStr for Ticket {
    type Err = anyhow::Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let bytes = hex::decode(value).context("decoding hex string")?;
        let ticket: Ticket = ciborium::de::from_reader(&bytes[..]).context("decoding CBOR")?;
        Ok(ticket)
    }
}

impl From<Ticket> for NodeAddress {
    fn from(value: Ticket) -> Self {
        value.0
    }
}
