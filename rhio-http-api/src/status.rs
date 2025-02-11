use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Clone, Default, Debug, JsonSchema, PartialEq)]
pub struct HealthStatus {
    pub status: ServiceStatus,
    pub msg: Option<String>,
    pub streams: MessageStreams,
    pub stores: ObjectStores,
}

impl From<anyhow::Error> for HealthStatus {
    fn from(error: anyhow::Error) -> Self {
        HealthStatus {
            status: ServiceStatus::Error,
            msg: Some(format!("{:?}", error)),
            ..HealthStatus::default()
        }
    }
}

#[derive(Deserialize, Serialize, Clone, Default, Debug, JsonSchema, PartialEq)]
pub enum ServiceStatus {
    Running,
    #[default]
    Unknown,
    Error,
}

#[derive(Deserialize, Serialize, Clone, Default, Debug, JsonSchema, PartialEq)]
pub struct MessageStreams {
    pub published: Vec<MessageStreamPublishStatus>,
    pub subscribed: Vec<MessageStreamSubscribeStatus>,
}

#[derive(Deserialize, Serialize, Clone, Default, Debug, JsonSchema, PartialEq)]
pub struct ObjectStores {
    pub published: Vec<ObjectStorePublishStatus>,
    pub subscribed: Vec<ObjectStoreSubscribeStatus>,
}

#[derive(Deserialize, Serialize, Clone, Default, Debug, JsonSchema, PartialEq)]
pub struct MessageStreamPublishStatus {
    pub stream: String,
    pub subject: String,
    pub status: ObjectStatus,
}

impl MessageStreamPublishStatus {
    pub fn to_unknown(stream: &String, subject: &String) -> MessageStreamPublishStatus {
        MessageStreamPublishStatus {
            status: ObjectStatus::Unknown,
            stream: stream.to_owned(),
            subject: subject.to_owned(),
        }
    }
}

#[derive(Deserialize, Serialize, Clone, Default, Debug, JsonSchema, PartialEq)]
pub struct MessageStreamSubscribeStatus {
    pub source: String,
    pub stream: String,
    pub subject: String,
    pub status: ObjectStatus,
}

impl MessageStreamSubscribeStatus {
    pub fn to_unknown(
        source: &String,
        stream: &String,
        subject: &String,
    ) -> MessageStreamSubscribeStatus {
        MessageStreamSubscribeStatus {
            status: ObjectStatus::Unknown,
            source: source.to_owned(),
            stream: stream.to_owned(),
            subject: subject.to_owned(),
        }
    }
}

#[derive(Deserialize, Serialize, Clone, Default, Debug, JsonSchema, PartialEq)]
pub struct ObjectStorePublishStatus {
    pub bucket: String,
    pub status: ObjectStatus,
}

impl ObjectStorePublishStatus {
    pub fn to_unknown(bucket: &String) -> ObjectStorePublishStatus {
        ObjectStorePublishStatus {
            status: ObjectStatus::Unknown,
            bucket: bucket.to_owned(),
        }
    }
}

#[derive(Deserialize, Serialize, Clone, Default, Debug, JsonSchema, PartialEq)]
pub struct ObjectStoreSubscribeStatus {
    pub source: String,
    pub remote_bucket: String,
    pub local_bucket: String,
    pub status: ObjectStatus,
}

impl ObjectStoreSubscribeStatus {
    pub fn to_unknown(
        source: &String,
        remote_bucket: &String,
        local_bucket: &String,
    ) -> ObjectStoreSubscribeStatus {
        ObjectStoreSubscribeStatus {
            status: ObjectStatus::Unknown,
            source: source.to_owned(),
            remote_bucket: remote_bucket.to_owned(),
            local_bucket: local_bucket.to_owned(),
        }
    }
}

#[derive(Deserialize, Serialize, Clone, Default, Debug, JsonSchema, PartialEq)]
pub enum ObjectStatus {
    #[default]
    Activated,
    Unknown,
}
