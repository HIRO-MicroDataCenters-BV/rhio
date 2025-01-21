use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use stackable_operator::kube::CustomResource;

#[derive(CustomResource, Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[cfg_attr(test, derive(Default))]
#[kube(
    kind = "ReplicatedMessageStreamSubscription",
    group = "rhio.hiro.io",
    version = "v1",
    namespaced,
    crates(
        kube_core = "stackable_operator::kube::core",
        k8s_openapi = "stackable_operator::k8s_openapi",
        schemars = "stackable_operator::schemars"
    )
)]
#[kube(
    status = "ReplicatedMessageStreamSubscriptionStatus",
    shortname = "ross"
)]
pub struct ReplicatedMessageStreamSubscriptionSpec {
    pub public_key: String,
    pub subscriptions: Vec<SubjectSpec>,
}

#[derive(Deserialize, Serialize, Clone, Default, Debug, JsonSchema)]
pub struct SubjectSpec {
    subject: String,
    stream: String,
}

#[derive(Deserialize, Serialize, Clone, Default, Debug, JsonSchema)]
pub struct ReplicatedMessageStreamSubscriptionStatus {}
