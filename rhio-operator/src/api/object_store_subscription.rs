use rhio_config::status::ObjectStoreSubscribeStatus;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use stackable_operator::kube::CustomResource;

#[derive(CustomResource, Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[cfg_attr(test, derive(Default))]
#[kube(
    kind = "ReplicatedObjectStoreSubscription",
    group = "rhio.hiro.io",
    version = "v1",
    plural = "replicatedobjectstoresubscriptions",
    status = "ReplicatedObjectStoreSubscriptionStatus",
    shortname = "ross",
    namespaced,
    crates(
        kube_core = "stackable_operator::kube::core",
        k8s_openapi = "stackable_operator::k8s_openapi",
        schemars = "stackable_operator::schemars"
    )
)]
#[serde(rename_all = "camelCase")]
pub struct ReplicatedObjectStoreSubscriptionSpec {
    #[serde(default)]
    pub public_key: String,
    pub buckets: Vec<BucketSpec>,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct BucketSpec {
    pub remote_bucket: String,
    pub local_bucket: String,
}

#[derive(Deserialize, Serialize, Clone, Default, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ReplicatedObjectStoreSubscriptionStatus {
    pub buckets: Vec<ObjectStoreSubscribeStatus>,
}
