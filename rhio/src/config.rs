use std::path::{absolute, PathBuf};
use std::str::FromStr;

use anyhow::Result;
use clap::Parser;
use figment::providers::{Env, Serialized};
use figment::Figment;
use p2panda_net::{Config as NetworkConfig, NodeAddress, RelayUrl};
use serde::{Deserialize, Serialize};

use crate::ticket::Ticket;

const DEFAULT_BLOBS_PATH: &str = "blobs";

// Use iroh's staging relay node for testing
const DEFAULT_RELAY_URL: &str = "https://staging-euw1-1.relay.iroh.network";

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Config {
    pub sync_dir: Option<PathBuf>,
    #[serde(flatten)]
    pub network_config: NetworkConfig,
}

impl Default for Config {
    fn default() -> Self {
        let network_config = NetworkConfig {
            relay: Some(DEFAULT_RELAY_URL.parse().expect("valid url")),
            ..NetworkConfig::default()
        };

        Self {
            sync_dir: None,
            network_config,
        }
    }
}

#[derive(Parser, Serialize, Debug)]
#[command(
    name = "rhio",
    about = "p2p blob syncing node for minio databases",
    long_about = None,
    version
)]
struct Cli {
    #[arg(short = 'p', long, value_name = "PORT")]
    #[serde(skip_serializing_if = "Option::is_none")]
    bind_port: Option<u16>,

    #[arg(short = 't', long, value_name = "TICKET", num_args = 0.., value_parser = parse_ticket)]
    #[serde(skip_serializing_if = "Option::is_none")]
    direct_node_addresses: Option<Vec<NodeAddress>>,

    #[arg(short = 'k', long, value_name = "PATH")]
    #[serde(skip_serializing_if = "Option::is_none")]
    private_key: Option<PathBuf>,

    #[arg(short = 'd', long, value_name = "PATH")]
    #[serde(skip_serializing_if = "Option::is_none")]
    sync_dir: Option<PathBuf>,

    #[arg(short = 'r', long, value_name = "URL", value_parser = parse_url)]
    #[serde(skip_serializing_if = "Option::is_none")]
    relay: Option<RelayUrl>,
}

pub fn parse_ticket(value: &str) -> Result<NodeAddress> {
    let ticket = Ticket::from_str(value)?;
    Ok(ticket.into())
}

pub fn parse_url(value: &str) -> Result<RelayUrl> {
    value.parse()
}

pub fn load_config() -> Result<Config> {
    let mut config: Config = Figment::new()
        .merge(Serialized::defaults(Config::default()))
        .merge(Env::raw())
        .merge(Serialized::defaults(Cli::parse()))
        .extract()?;

    // Make blobs path absolute.
    let absolute_path = config
        .sync_dir
        .map(|path| absolute(path).expect("to establish absolute path"));
    config.sync_dir = absolute_path;

    Ok(config)
}
