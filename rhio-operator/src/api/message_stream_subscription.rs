use rhio_http_api::status::MessageStreamSubscribeStatus;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use stackable_operator::kube::CustomResource;

#[derive(CustomResource, Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[cfg_attr(test, derive(Default))]
#[kube(
    kind = "ReplicatedMessageStreamSubscription",
    group = "rhio.hiro.io",
    version = "v1",
    plural = "replicatedmessagestreamsubscriptions",
    status = "ReplicatedMessageStreamSubscriptionStatus",
    shortname = "rmss",
    namespaced,
    crates(
        kube_core = "stackable_operator::kube::core",
        k8s_openapi = "stackable_operator::k8s_openapi",
        schemars = "stackable_operator::schemars"
    )
)]
#[serde(rename_all = "camelCase")]
pub struct ReplicatedMessageStreamSubscriptionSpec {
    pub public_key: String,
    pub subscriptions: Vec<SubjectSpec>,
}

#[derive(Deserialize, Serialize, Clone, Default, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SubjectSpec {
    pub subject: String,
    pub stream: String,
}

#[derive(Deserialize, Serialize, Clone, Default, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ReplicatedMessageStreamSubscriptionStatus {
    pub subjects: Vec<MessageStreamSubscribeStatus>,
}
