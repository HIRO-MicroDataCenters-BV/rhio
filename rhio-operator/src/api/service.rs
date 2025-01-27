use rhio_config::status::HealthStatus;
use rhio_config::status::MessageStreamPublishStatus;
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
    pub cluster_config: RhioClusterConfig,
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
    pub fn server_rolegroup_ref(&self, group_name: impl Into<String>) -> RoleGroupRef<RhioService> {
        RoleGroupRef {
            cluster: ObjectRef::from_obj(self),
            role: RhioRole::Server.to_string(),
            role_group: group_name.into(),
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
pub struct RhioClusterConfig {
    /// This field controls which type of Service the Operator creates for this ZookeeperCluster:
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
    pub fn status_for(&self, stream: &ReplicatedMessageStream) -> Vec<MessageStreamPublishStatus> {
        let mut results = vec![];
        let statuses = &self.status.streams.published;
        for subject in stream.spec.subjects.iter() {
            for published in statuses {
                if published.stream == stream.spec.stream_name && subject == &published.subject {
                    results.push(published.to_owned())
                }
            }
        }
        results
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
