use std::path::PathBuf;

use rhio::config::{
    parse_bucket_address, parse_s3_credentials, parse_ticket, parse_url, Config as InnerConfig,
};

use crate::error::RhioError;
use crate::types::{ImportPath, Path};

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
    pub import_path: Option<ImportPath>,
    #[uniffi(default = None)]
    pub bucket_name: Option<String>,
    #[uniffi(default = None)]
    pub bucket_address: Option<String>,
    #[uniffi(default = None)]
    pub credentials: Option<String>,
    #[uniffi(default = None)]
    pub relay: Option<String>,
}

#[derive(Clone, Debug, Default, uniffi::Object)]
pub struct Config {
    pub inner: InnerConfig,
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

        inner.import_path = cli.import_path.map(Into::into);

        if let Some(bucket_name) = cli.bucket_name {
            inner.bucket_name = bucket_name
        };

        if let Some(bucket_address) = cli.bucket_address {
            inner.bucket_address =
                parse_bucket_address(&bucket_address).expect("invalid bucket address")
        };

        if let Some(credentials_str) = cli.credentials {
            inner.credentials = parse_s3_credentials(&credentials_str).expect("invalid import path")
        };

        inner.network_config.relay = cli
            .relay
            .map(|url_str| parse_url(&url_str).expect("invalid relay url"));

        Ok(Self { inner })
    }
}

impl Into<InnerConfig> for Config {
    fn into(self) -> InnerConfig {
        self.inner
    }
}
