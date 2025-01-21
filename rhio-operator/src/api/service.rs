use std::collections::BTreeMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use stackable_operator::commons::cluster_operation::ClusterOperation;
use stackable_operator::commons::product_image_selection::ProductImage;
use stackable_operator::config::fragment::Fragment;
use stackable_operator::config::merge::Merge;
use stackable_operator::kube::CustomResource;
use stackable_operator::product_config_utils::Configuration;
use stackable_operator::status::condition::ClusterCondition;
use stackable_operator::status::condition::HasStatusCondition;

#[derive(CustomResource, Deserialize, Serialize, Clone, Debug, JsonSchema)]
#[kube(
    kind = "RhioService",
    group = "rhio.hiro.io",
    version = "v1",
    status = "RhioServiceStatus",
    shortname = "rhio",
    plural = "rhioservices",
    namespaced,
    crates(
        kube_core = "stackable_operator::kube::core",
        k8s_openapi = "stackable_operator::k8s_openapi",
        schemars = "stackable_operator::schemars"
    )
)]
pub struct RhioServiceSpec {
    pub image: ProductImage,
    pub configuration: RhioConfig,
    pub app_version_label: String,
    pub nodes: Vec<NodePeerConfigSpec>,
    pub network_id: String,
    pub s3: S3ConfigSpec,
    pub nats: NatsConfigSpec,
    #[serde(default)]
    pub cluster_operation: ClusterOperation,
    pub status: Option<RhioServiceStatus>,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct S3ConfigSpec {
    pub endpoint: String,
    pub region: String,
    pub credential_secret: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct NatsConfigSpec {
    pub endpoint: String,
    pub credential_secret: Option<String>,
}

#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct NodePeerConfigSpec {
    pub public_key: String,
    pub endpoints: Vec<String>,
}

#[derive(Deserialize, Serialize, Clone, Default, Debug, JsonSchema)]
pub struct RhioServiceStatus {
    #[serde(default)]
    pub conditions: Vec<ClusterCondition>,
}

impl HasStatusCondition for RhioServiceSpec {
    fn conditions(&self) -> Vec<ClusterCondition> {
        match &self.status {
            Some(status) => status.conditions.clone(),
            None => vec![],
        }
    }
}

#[derive(Clone, Deserialize, Debug, Eq, JsonSchema, PartialEq, Fragment, Serialize, Default)]
#[fragment_attrs(
    derive(Clone, Debug, Default, Deserialize, JsonSchema, PartialEq, Serialize),
    serde(rename_all = "camelCase")
)]
pub struct RhioConfig {
    pub bind_port: u16,
    pub http_bind_port: u16,
    pub network_id: String,

    #[fragment_attrs(serde(default))]
    pub private_key_path: String,

    #[fragment_attrs(serde(default))]
    pub s3: Option<S3Config>,

    #[fragment_attrs(serde(default))]
    pub nats: Option<NatsConfig>,
}

impl RhioConfig {
    pub fn default_config(network_name: &str) -> RhioConfigFragment {
        RhioConfigFragment {
            bind_port: Some(9102),
            http_bind_port: Some(8080),
            network_id: Some(network_name.into()),
            private_key_path: Some("/etc/rhio/private_key.txt".into()),
            s3: None,
            nats: None,
        }
    }
}

impl Configuration for RhioConfigFragment {
    type Configurable = RhioConfig;

    fn compute_env(
        &self,
        _resource: &Self::Configurable,
        _role_name: &str,
    ) -> Result<BTreeMap<String, Option<String>>, stackable_operator::product_config_utils::Error>
    {
        Ok(BTreeMap::new())
    }

    fn compute_cli(
        &self,
        _resource: &Self::Configurable,
        _role_name: &str,
    ) -> Result<BTreeMap<String, Option<String>>, stackable_operator::product_config_utils::Error>
    {
        Ok(BTreeMap::new())
    }

    fn compute_files(
        &self,
        _resource: &Self::Configurable,
        _role_name: &str,
        _file: &str,
    ) -> Result<BTreeMap<String, Option<String>>, stackable_operator::product_config_utils::Error>
    {
        let config = BTreeMap::new();

        Ok(config)
    }
}

#[derive(Clone, Deserialize, Debug, Eq, JsonSchema, PartialEq, Fragment, Serialize, Default)]
#[fragment_attrs(
    derive(Clone, Debug, Default, Deserialize, JsonSchema, PartialEq, Serialize),
    serde(rename_all = "camelCase")
)]
pub struct S3Config {
    pub endpoint: String,
    pub region: String,
    pub credentials: Option<S3Credentials>,
}

#[derive(Clone, Deserialize, Debug, Eq, JsonSchema, PartialEq, Fragment, Serialize, Default)]
#[fragment_attrs(
    derive(
        Clone,
        Debug,
        Default,
        Deserialize,
        JsonSchema,
        Merge,
        PartialEq,
        Serialize
    ),
    serde(rename_all = "camelCase")
)]
pub struct S3Credentials {
    pub access_key: String,
    pub secret_key: String,
}

#[derive(Clone, Deserialize, Debug, Eq, JsonSchema, PartialEq, Fragment, Serialize, Default)]
#[fragment_attrs(
    derive(
        Clone,
        Debug,
        Default,
        Deserialize,
        JsonSchema,
        // Merge,
        PartialEq,
        Serialize
    ),
    serde(rename_all = "camelCase")
)]
pub struct NatsConfig {
    pub endpoint: String,

    pub credentials: Option<NatsCredentials>,
}

#[derive(Clone, Deserialize, Debug, Eq, JsonSchema, PartialEq, Fragment, Serialize, Default)]
#[fragment_attrs(
    derive(
        Clone,
        Debug,
        Default,
        Deserialize,
        JsonSchema,
        Merge,
        PartialEq,
        Serialize
    ),
    serde(rename_all = "camelCase")
)]

pub struct NatsCredentials {
    pub username: String,
    pub password: String,
}
