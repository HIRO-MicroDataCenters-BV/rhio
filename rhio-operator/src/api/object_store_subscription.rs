use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use stackable_operator::kube::CustomResource;

#[derive(CustomResource, Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[cfg_attr(test, derive(Default))]
#[kube(
    kind = "ReplicatedObjectStoreSubscription",
    group = "rhio.io",
    version = "v1",
    namespaced,
    crates(
        kube_core = "stackable_operator::kube::core",
        k8s_openapi = "stackable_operator::k8s_openapi",
        schemars = "stackable_operator::schemars"
    )
)]
#[kube(status = "ReplicatedObjectStoreSubscriptionStatus", shortname = "ross")]
pub struct ReplicatedObjectStoreSubscriptionSpec {
    pub subscriptions: Vec<SubscriptionsSpec>,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct SubscriptionsSpec {
    pub public_key: String,
    pub buckets: Vec<String>,
}

#[derive(Deserialize, Serialize, Clone, Default, Debug, JsonSchema)]
pub struct ReplicatedObjectStoreSubscriptionStatus {}
