// SPDX-License-Identifier: AGPL-3.0-or-later

use anyhow::Result;
use iroh_gossip::proto::TopicId;
use rand::random;
use serde::{Deserialize, Serialize};

pub type MessageId = [u8; 32];

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum NetworkMessage {
    Announcement(MessageId, Vec<TopicId>),
}

impl NetworkMessage {
    pub fn new_announcement(topics: Vec<TopicId>) -> Self {
        // Message id is used to make every message unique, as duplicates get otherwise dropped
        // during gossip broadcast.
        let message_id = random();
        NetworkMessage::Announcement(message_id, topics)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let message: Self = ciborium::de::from_reader(bytes)?;
        Ok(message)
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        let mut bytes: Vec<u8> = Vec::new();
        ciborium::ser::into_writer(&self, &mut bytes)?;
        Ok(bytes)
    }
}
