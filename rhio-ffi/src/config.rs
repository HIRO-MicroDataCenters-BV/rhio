use std::path::PathBuf;

use rhio::config::{parse_ticket, parse_url, Config as RhioConfig};

use crate::error::RhioError;

#[derive(Default, Clone, uniffi::Record)]
pub struct Config {
    #[uniffi(default = None)]
    pub sync_dir: Option<String>,
    #[uniffi(default = 2024)]
    pub bind_port: u16,
    #[uniffi(default = None)]
    pub private_key: Option<String>,
    #[uniffi(default = [])]
    pub ticket: Vec<String>,
    #[uniffi(default = None)]
    pub relay: Option<String>,
}

impl TryInto<RhioConfig> for Config {
    type Error = RhioError;

    fn try_into(self) -> Result<RhioConfig, Self::Error> {
        let mut config = RhioConfig::default();
        if let Some(path) = self.sync_dir {
            config.sync_dir = Some(PathBuf::from(&path));
        };

        config.network_config.bind_port = self.bind_port;

        if let Some(path) = self.private_key {
            config.network_config.private_key = Some(PathBuf::from(path));
        }

        config.network_config.direct_node_addresses = self
            .ticket
            .iter()
            .map(|addr| parse_ticket(addr))
            .collect::<Result<Vec<_>, _>>()?;

        config.network_config.relay = self
            .relay
            .map(|url_str| parse_url(&url_str).expect("invalid relay url"));
        Ok(config)
    }
}
