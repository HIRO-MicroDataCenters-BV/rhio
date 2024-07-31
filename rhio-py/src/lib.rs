use std::path::PathBuf;

use rhio::config::{parse_node_addr, parse_url, Config as RhioConfig, NodeAddr};
use rhio::private_key::generate_ephemeral_private_key;
use rhio::Node as RhioNode;
use uniffi;

uniffi::setup_scaffolding!();

#[derive(Debug, thiserror::Error, uniffi::Object)]
#[error("{e:?}")]
#[uniffi::export(Debug)]
pub struct RhioError {
    e: anyhow::Error,
}

#[uniffi::export]
impl RhioError {
    pub fn message(&self) -> String {
        self.to_string()
    }
}

impl From<anyhow::Error> for RhioError {
    fn from(e: anyhow::Error) -> Self {
        Self { e }
    }
}

#[derive(uniffi::Object)]
pub struct Node {
    pub inner: RhioNode,
}

#[derive(Clone, uniffi::Record)]
pub struct Config {
    #[uniffi(default = None)]
    pub blobs_path: Option<String>,
    #[uniffi(default = 2024)]
    pub bind_port: u16,
    #[uniffi(default = None)]
    pub private_key: Option<String>,
    #[uniffi(default = [])]
    pub direct_node_addresses: Vec<String>,
    #[uniffi(default = [])]
    pub relay_addresses: Vec<String>,
}

impl TryInto<RhioConfig> for Config {
    type Error = RhioError;

    fn try_into(self) -> Result<RhioConfig, Self::Error> {
        let mut config = RhioConfig::default();
        if let Some(path) = self.blobs_path {
            config.blobs_path = Some(PathBuf::from(&path));
        };

        config.network_config.bind_port = self.bind_port;

        if let Some(path) = self.private_key {
            config.network_config.private_key = Some(PathBuf::from(path));
        }

        config.network_config.direct_node_addresses = self
            .direct_node_addresses
            .iter()
            .map(|addr| parse_node_addr(addr))
            .collect::<Result<Vec<NodeAddr>, _>>()?;

        config.network_config.relay_addresses = self
            .relay_addresses
            .iter()
            .map(|url_str| parse_url(url_str))
            .collect::<Result<_, _>>()?;
        Ok(config)
    }
}

#[uniffi::export]
impl Node {
    #[uniffi::constructor(async_runtime = "tokio")]
    pub async fn spawn(config: &Config) -> Result<Node, RhioError> {
        let private_key = generate_ephemeral_private_key();
        let config: RhioConfig = config.clone().try_into()?;
        let rhio_node = RhioNode::spawn(config, private_key).await?;
        Ok(Node { inner: rhio_node })
    }

    #[uniffi::method]
    pub fn id(&self) -> String {
        self.inner.id().to_hex()
    }
}
