use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Clone, Default, Debug, JsonSchema)]
pub enum ServiceStatus {
    #[default]
    Running,
    Unknown,
    Error,
}

#[derive(Deserialize, Serialize, Clone, Default, Debug, JsonSchema)]
pub enum ObjectStatus {
    #[default]
    Activated,
}

#[derive(Deserialize, Serialize, Clone, Default, Debug, JsonSchema)]
pub struct MessageStreamPublishStatus {
    pub stream: String,
    pub subject: String,
    pub status: ObjectStatus,
}

#[derive(Deserialize, Serialize, Clone, Default, Debug, JsonSchema)]
pub struct MessageStreamSubscribeStatus {
    pub source: String,
    pub stream: String,
    pub subject: String,
    pub status: ObjectStatus,
}

#[derive(Deserialize, Serialize, Clone, Default, Debug, JsonSchema)]
pub struct ObjectStorePublishStatus {
    pub bucket: String,
    pub status: ObjectStatus,
}

#[derive(Deserialize, Serialize, Clone, Default, Debug, JsonSchema)]
pub struct ObjectStoreSubscribeStatus {
    pub source: String,
    pub remote_bucket: String,
    pub local_bucket: String,
    pub status: ObjectStatus,
}

#[derive(Deserialize, Serialize, Clone, Default, Debug, JsonSchema)]
pub struct MessageStreams {
    pub published: Vec<MessageStreamPublishStatus>,
    pub subscribed: Vec<MessageStreamSubscribeStatus>,
}

#[derive(Deserialize, Serialize, Clone, Default, Debug, JsonSchema)]
pub struct ObjectStores {
    pub published: Vec<ObjectStorePublishStatus>,
    pub subscribed: Vec<ObjectStoreSubscribeStatus>,
}

#[derive(Deserialize, Serialize, Clone, Default, Debug, JsonSchema)]
pub struct HealthStatus {
    pub streams: MessageStreams,
    pub stores: ObjectStores,
    pub status: ServiceStatus,
    pub msg: Option<String>,
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
