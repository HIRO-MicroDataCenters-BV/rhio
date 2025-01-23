use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use stackable_operator::{crd::ClusterRef, kube::CustomResource};

use super::service::{HasServiceRef, RhioService};

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
    #[serde(default)]
    pub service_ref: ClusterRef<RhioService>,
    pub public_key: String,
    pub subscriptions: Vec<SubjectSpec>,
}

impl HasServiceRef for ReplicatedMessageStreamSubscription {
    fn service_ref(&self) -> ClusterRef<RhioService> {
        self.spec.service_ref.clone()
    }
}

#[derive(Deserialize, Serialize, Clone, Default, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct SubjectSpec {
    pub subject: String,
    pub stream: String,
}

#[derive(Deserialize, Serialize, Clone, Default, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ReplicatedMessageStreamSubscriptionStatus {}
