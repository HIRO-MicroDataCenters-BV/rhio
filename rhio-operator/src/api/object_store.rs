use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use stackable_operator::kube::CustomResource;

/// Generate the Kubernetes wrapper struct `ReplicatedObjectStore` from our Spec and Status struct
///
/// This provides a hook for generating the CRD yaml (in crdgen.rs)
#[derive(CustomResource, Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[cfg_attr(test, derive(Default))]
#[kube(
    kind = "ReplicatedObjectStore",
    group = "rhio.io",
    version = "v1",
    namespaced,
    crates(
        kube_core = "stackable_operator::kube::core",
        k8s_openapi = "stackable_operator::k8s_openapi",
        schemars = "stackable_operator::schemars"
    )
)]
#[kube(status = "ReplicatedObjectStoreStatus", shortname = "ros")]
#[serde(rename_all = "camelCase")]
pub struct ReplicatedObjectStoreSpec {
    pub buckets: Vec<String>,
}

#[derive(Deserialize, Serialize, Clone, Default, Debug, JsonSchema)]
pub struct ReplicatedObjectStoreStatus {}
