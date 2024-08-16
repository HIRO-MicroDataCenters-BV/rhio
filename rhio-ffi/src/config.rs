use std::path::PathBuf;

use rhio::{
    config::{
        parse_import_path, parse_s3_credentials, parse_ticket, parse_url, Config as RhioConfig,
    },
    Credentials,
};

use crate::error::RhioError;

#[derive(Default, Clone, uniffi::Record)]
pub struct Config {
    #[uniffi(default = 2024)]
    pub bind_port: u16,
    #[uniffi(default = None)]
    pub private_key: Option<String>,
    #[uniffi(default = [])]
    pub ticket: Vec<String>,
    #[uniffi(default = None)]
    pub sync_dir: Option<String>,
    #[uniffi(default = None)]
    pub blobs_dir: Option<String>,
    #[uniffi(default = None)]
    pub import_path: Option<String>,
    #[uniffi(default = None)]
    pub credentials: Option<String>,
    #[uniffi(default = None)]
    pub relay: Option<String>,
}

impl TryInto<RhioConfig> for Config {
    type Error = RhioError;

    fn try_into(self) -> Result<RhioConfig, Self::Error> {
        let mut config = RhioConfig::default();

        config.network_config.bind_port = self.bind_port;

        config.network_config.private_key = self.private_key.map(PathBuf::from);

        config.network_config.direct_node_addresses = self
            .ticket
            .iter()
            .map(|addr| parse_ticket(addr).map(Into::into))
            .collect::<Result<Vec<_>, _>>()?;

        config.sync_dir = self.sync_dir.map(PathBuf::from);

        config.blobs_dir = self.blobs_dir.map(PathBuf::from);

        config.import_path = self
            .import_path
            .map(|path_str| parse_import_path(&path_str).expect("invalid import path"));

        config.credentials = if let Some(credentials_str) = self.credentials {
            parse_s3_credentials(&credentials_str).expect("invalid import path")
        } else {
            Credentials::anonymous().expect("error constructing anonymous credentials")
        };

        config.network_config.relay = self
            .relay
            .map(|url_str| parse_url(&url_str).expect("invalid relay url"));
        Ok(config)
    }
}
