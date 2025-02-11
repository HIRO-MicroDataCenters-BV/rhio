use rhio_http_api::status::ObjectStorePublishStatus;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use stackable_operator::kube::CustomResource;

#[derive(CustomResource, Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[cfg_attr(test, derive(Default))]
#[kube(
    kind = "ReplicatedObjectStore",
    group = "rhio.hiro.io",
    version = "v1",
    plural = "replicatedobjectstores",
    status = "ReplicatedObjectStoreStatus",
    shortname = "ros",
    namespaced,
    crates(
        kube_core = "stackable_operator::kube::core",
        k8s_openapi = "stackable_operator::k8s_openapi",
        schemars = "stackable_operator::schemars"
    )
)]
#[serde(rename_all = "camelCase")]
pub struct ReplicatedObjectStoreSpec {
    pub buckets: Vec<String>,
}

#[derive(Deserialize, Serialize, Clone, Default, Debug, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ReplicatedObjectStoreStatus {
    pub buckets: Vec<ObjectStorePublishStatus>,
}
