use rhio_config::status::MessageStreamPublishStatus;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use stackable_operator::kube::CustomResource;

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
    pub stream_name: String,
    pub subjects: Vec<String>,
}

#[derive(Deserialize, Serialize, Clone, Default, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ReplicatedMessageStreamStatus {
    pub subjects: Vec<MessageStreamPublishStatus>,
}
