use std::path::PathBuf;

use rhio::config::{parse_s3_credentials, parse_ticket, parse_url, Config as InnerConfig};

use crate::error::RhioError;
use crate::types::Path;

#[derive(Default, Clone, uniffi::Record)]
pub struct Cli {
    #[uniffi(default = None)]
    pub bind_port: Option<u16>,
    #[uniffi(default = None)]
    pub private_key: Option<Path>,
    #[uniffi(default = [])]
    pub ticket: Vec<String>,
    #[uniffi(default = None)]
    pub sync_dir: Option<String>,
    #[uniffi(default = None)]
    pub blobs_dir: Option<String>,
    #[uniffi(default = None)]
    pub minio_bucket_name: Option<String>,
    #[uniffi(default = None)]
    pub minio_endpoint: Option<String>,
    #[uniffi(default = None)]
    pub minio_region: Option<String>,
    #[uniffi(default = None)]
    pub minio_credentials: Option<String>,
    #[uniffi(default = None)]
    pub relay: Option<String>,
}

#[derive(Clone, Debug, Default, uniffi::Object)]
pub struct Config {
    pub inner: InnerConfig,
}

#[uniffi::export]
impl Config {
    pub fn minio_bucket_name(&self) -> String {
        self.inner.minio.bucket_name.to_owned()
    }

    pub fn minio_endpoint(&self) -> String {
        self.inner.minio.endpoint.to_owned()
    }

    pub fn minio_region(&self) -> String {
        self.inner.minio.region.to_owned()
    }
}

#[uniffi::export]
impl Config {
    #[uniffi::constructor]
    pub fn from_cli(cli: Cli) -> Result<Self, RhioError> {
        let mut inner = InnerConfig::default();

        if let Some(bind_port) = cli.bind_port {
            inner.network_config.bind_port = bind_port;
        }

        inner.network_config.private_key = cli.private_key.map(PathBuf::from);

        inner.network_config.direct_node_addresses = cli
            .ticket
            .iter()
            .map(|addr| parse_ticket(addr).map(Into::into))
            .collect::<Result<Vec<_>, _>>()?;

        inner.sync_dir = cli.sync_dir.map(PathBuf::from);

        inner.blobs_dir = cli.blobs_dir.map(PathBuf::from);

        if let Some(bucket_name) = cli.minio_bucket_name {
            inner.minio.bucket_name = bucket_name
        };

        if let Some(minio_endpoint) = cli.minio_endpoint {
            inner.minio.endpoint = minio_endpoint
        };

        if let Some(minio_region) = cli.minio_region {
            inner.minio.region = minio_region
        };

        if let Some(credentials_str) = cli.minio_credentials {
            inner.minio.credentials =
                Some(parse_s3_credentials(&credentials_str).expect("invalid credentials"))
        };

        inner.network_config.relay = cli
            .relay
            .map(|url_str| parse_url(&url_str).expect("invalid relay url"));

        Ok(Self { inner })
    }
}

impl From<Config> for InnerConfig {
    fn from(value: Config) -> Self {
        value.inner
    }
}
