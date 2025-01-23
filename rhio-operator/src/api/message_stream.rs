use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use stackable_operator::{crd::ClusterRef, kube::CustomResource};

use super::service::{HasServiceRef, RhioService};

#[derive(CustomResource, Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[cfg_attr(test, derive(Default))]
#[kube(
    kind = "ReplicatedMessageStream",
    group = "rhio.hiro.io",
    version = "v1",
    plural = "replicatedmessagestreams",
    namespaced,
    status = "ReplicatedMessageStreamStatus",
    shortname = "rms",
    crates(
        kube_core = "stackable_operator::kube::core",
        k8s_openapi = "stackable_operator::k8s_openapi",
        schemars = "stackable_operator::schemars"
    )
)]
#[serde(rename_all = "camelCase")]
pub struct ReplicatedMessageStreamSpec {
    #[serde(default)]
    pub service_ref: ClusterRef<RhioService>,
    pub stream_name: String,
    pub subjects: Vec<String>,
}

impl HasServiceRef for ReplicatedMessageStream {
    fn service_ref(&self) -> ClusterRef<RhioService> {
        self.spec.service_ref.clone()
    }
}

#[derive(Deserialize, Serialize, Clone, Default, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ReplicatedMessageStreamStatus {}
