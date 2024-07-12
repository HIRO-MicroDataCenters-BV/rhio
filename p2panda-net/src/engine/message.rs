// SPDX-License-Identifier: AGPL-3.0-or-later

use anyhow::Result;
use iroh_gossip::proto::TopicId;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum NetworkMessage {
    Announcement(Vec<TopicId>),
}

impl NetworkMessage {
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let message: Self = ciborium::de::from_reader(&bytes[..])?;
        Ok(message)
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        let mut bytes: Vec<u8> = Vec::new();
        ciborium::ser::into_writer(&self, &mut bytes)?;
        Ok(bytes)
    }
}
