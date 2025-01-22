use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use stackable_operator::{crd::ClusterRef, kube::CustomResource};

use super::service::RhioService;

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
pub struct ReplicatedMessageStreamSpec {
    #[serde(default)]
    pub service_ref: ClusterRef<RhioService>,

    pub public_key: String,
    pub subjects: Vec<String>,
}

#[derive(Deserialize, Serialize, Clone, Default, Debug, JsonSchema)]
pub struct ReplicatedMessageStreamStatus {}
