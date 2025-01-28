use rhio_http_api::status::HealthStatus;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use stackable_operator::commons::affinity::StackableAffinity;
use stackable_operator::commons::cluster_operation::ClusterOperation;
use stackable_operator::commons::product_image_selection::ProductImage;
use stackable_operator::crd::ClusterRef;
use stackable_operator::kube::runtime::reflector::ObjectRef;
use stackable_operator::kube::CustomResource;
use stackable_operator::role_utils::RoleGroupRef;
use stackable_operator::status::condition::ClusterCondition;
use stackable_operator::status::condition::HasStatusCondition;
use stackable_operator::time::Duration;
use strum::Display;

use super::message_stream::ReplicatedMessageStream;
use super::message_stream::ReplicatedMessageStreamStatus;
use super::message_stream_subscription::ReplicatedMessageStreamSubscription;
use super::message_stream_subscription::ReplicatedMessageStreamSubscriptionStatus;
use super::object_store::ReplicatedObjectStore;
use super::object_store::ReplicatedObjectStoreStatus;
use super::object_store_subscription::ReplicatedObjectStoreSubscription;
use super::object_store_subscription::ReplicatedObjectStoreSubscriptionStatus;
use super::role::RhioRole;

#[derive(CustomResource, Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[kube(
    kind = "RhioService",
    group = "rhio.hiro.io",
    version = "v1",
    status = "RhioServiceStatus",
    shortname = "rhio",
    plural = "rhioservices",
    namespaced,
    crates(
        kube_core = "stackable_operator::kube::core",
        k8s_openapi = "stackable_operator::k8s_openapi",
        schemars = "stackable_operator::schemars"
    )
)]
#[serde(rename_all = "camelCase")]
pub struct RhioServiceSpec {
    pub image: ProductImage,
    pub cluster_config: RhioServiceConfig,
    pub configuration: RhioConfig,
    #[serde(default)]
    pub cluster_operation: ClusterOperation,
    pub status: Option<RhioServiceStatus>,
}

impl RhioService {
    /// The name of the role-level load-balanced Kubernetes `Service`
    pub fn server_role_service_name(&self) -> Option<String> {
        self.metadata.name.clone()
    }

    /// Metadata about a server rolegroup
    pub fn server_rolegroup_ref(&self) -> RoleGroupRef<RhioService> {
        RoleGroupRef {
            cluster: ObjectRef::from_obj(self),
            role: RhioRole::Server.to_string(),
            role_group: "default".into(),
        }
    }
    pub fn service_ref(&self) -> ClusterRef<RhioService> {
        ClusterRef::to_object(self)
    }
}

#[derive(Clone, Debug, Default, Display, Deserialize, Eq, JsonSchema, PartialEq, Serialize)]
#[serde(rename_all = "PascalCase")]
pub enum CurrentlySupportedListenerClasses {
    #[default]
    #[serde(rename = "cluster-internal")]
    ClusterInternal,
    #[serde(rename = "external-unstable")]
    ExternalUnstable,
}

impl CurrentlySupportedListenerClasses {
    pub fn k8s_service_type(&self) -> String {
        match self {
            CurrentlySupportedListenerClasses::ClusterInternal => "ClusterIP".to_string(),
            CurrentlySupportedListenerClasses::ExternalUnstable => "NodePort".to_string(),
        }
    }
}

#[derive(Clone, Deserialize, Debug, JsonSchema, Serialize, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct RhioServiceConfig {
    /// This field controls which type of Service the Operator creates for this RhioService:
    ///
    /// * cluster-internal: Use a ClusterIP service
    ///
    /// * external-unstable: Use a NodePort service
    ///
    #[serde(default)]
    pub listener_class: CurrentlySupportedListenerClasses,

    pub affinity: Option<StackableAffinity>,

    /// Time period Pods have to gracefully shut down, e.g. `30m`, `1h` or `2d`. Consult the operator documentation for details.
    pub graceful_shutdown_timeout: Option<Duration>,
}

#[derive(Clone, Deserialize, Debug, Eq, JsonSchema, PartialEq, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct RhioConfig {
    pub bind_port: u16,
    pub http_bind_port: u16,
    pub network_id: String,
    pub private_key_secret: String,
    pub nodes: Vec<NodePeerConfigSpec>,
    pub s3: Option<S3ConfigSpec>,
    pub nats: NatsConfigSpec,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, Default, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct S3ConfigSpec {
    pub endpoint: String,
    pub region: String,
    pub credentials: Option<S3Credentials>,
}

#[derive(Clone, Deserialize, Debug, Eq, JsonSchema, PartialEq, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct S3Credentials {
    pub access_key: String,
    pub secret_key: String,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, Default, Eq, PartialEq)]
#[serde(rename_all = "camelCase")]
pub struct NatsConfigSpec {
    pub endpoint: String,
    pub credentials: Option<NatsCredentials>,
}

#[derive(Clone, Deserialize, Debug, Eq, JsonSchema, PartialEq, Serialize, Default)]
#[serde(rename_all = "camelCase")]

pub struct NatsCredentials {
    pub username: String,
    pub password: String,
}

#[derive(Deserialize, Serialize, Clone, Default, Debug, JsonSchema)]
pub struct RhioServiceStatus {
    #[serde(default)]
    pub conditions: Vec<ClusterCondition>,
    pub status: HealthStatus,
}

impl RhioServiceStatus {
    pub fn stream_status(&self, stream: &ReplicatedMessageStream) -> ReplicatedMessageStreamStatus {
        let mut results = vec![];
        let statuses = &self.status.streams.published;
        for subject in stream.spec.subjects.iter() {
            for published in statuses {
                if published.stream == stream.spec.stream_name && subject == &published.subject {
                    results.push(published.to_owned())
                }
            }
        }
        ReplicatedMessageStreamStatus { subjects: results }
    }

    pub fn stream_subscription_status(
        &self,
        stream: &ReplicatedMessageStreamSubscription,
    ) -> ReplicatedMessageStreamSubscriptionStatus {
        let mut results = vec![];
        let statuses = &self.status.streams.subscribed;
        for subject in stream.spec.subscriptions.iter() {
            for subscription in statuses {
                if subject.stream == subscription.stream
                    && subject.subject == subscription.subject
                    && stream.spec.public_key == subscription.source
                {
                    results.push(subscription.to_owned())
                }
            }
        }
        ReplicatedMessageStreamSubscriptionStatus { subjects: results }
    }

    pub fn store_status(&self, store: &ReplicatedObjectStore) -> ReplicatedObjectStoreStatus {
        let mut results = vec![];
        let statuses = &self.status.stores.published;
        for bucket in store.spec.buckets.iter() {
            for published in statuses {
                if &published.bucket == bucket {
                    results.push(published.to_owned())
                }
            }
        }
        ReplicatedObjectStoreStatus { buckets: results }
    }

    pub fn store_subscription_status(
        &self,
        subscription: &ReplicatedObjectStoreSubscription,
    ) -> ReplicatedObjectStoreSubscriptionStatus {
        let mut results = vec![];
        let statuses = &self.status.stores.subscribed;
        for bucket in subscription.spec.buckets.iter() {
            for subscription_status in statuses {
                if subscription_status.source == subscription.spec.public_key
                    && subscription_status.local_bucket == bucket.local_bucket
                    && subscription_status.remote_bucket == bucket.remote_bucket
                {
                    results.push(subscription_status.to_owned())
                }
            }
        }
        ReplicatedObjectStoreSubscriptionStatus { buckets: results }
    }
}

impl HasStatusCondition for RhioService {
    fn conditions(&self) -> Vec<ClusterCondition> {
        match &self.status {
            Some(status) => status.conditions.clone(),
            None => vec![],
        }
    }
}

#[derive(Clone, Deserialize, Debug, Eq, JsonSchema, PartialEq, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct NodePeerConfigSpec {
    pub public_key: String,
    pub endpoints: Vec<String>,
}

pub trait HasServiceRef {
    fn service_ref(&self) -> ClusterRef<RhioService>;
}
