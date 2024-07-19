use std::path::PathBuf;
use std::str::FromStr;

use anyhow::Result;
use clap::Parser;
use figment::providers::{Env, Serialized};
use figment::Figment;
use p2panda_net::{Config as NetworkConfig, NodeAddress, RelayUrl};
use serde::{Deserialize, Serialize};

use crate::ticket::Ticket;

const DEFAULT_BLOBS_PATH: &str = "./blobs";

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Config {
    pub blobs_path: PathBuf,
    #[serde(flatten)]
    pub network_config: NetworkConfig,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            blobs_path: DEFAULT_BLOBS_PATH.into(),
            network_config: NetworkConfig::default(),
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
    tickets: Option<Vec<NodeAddress>>,

    #[arg(short = 'k', long, value_name = "PATH")]
    #[serde(skip_serializing_if = "Option::is_none")]
    private_key: Option<PathBuf>,

    #[arg(short = 'b', long, value_name = "PATH")]
    #[serde(skip_serializing_if = "Option::is_none")]
    blobs_path: Option<PathBuf>,

    #[arg(short = 'r', long, value_name = "URL", num_args = 0.., value_parser = parse_url)]
    #[serde(skip_serializing_if = "Option::is_none")]
    relay_addresses: Option<Vec<RelayUrl>>,
}

fn parse_ticket(value: &str) -> Result<NodeAddress> {
    let ticket = Ticket::from_str(value)?;
    Ok(ticket.into())
}

fn parse_url(value: &str) -> Result<RelayUrl> {
    value.parse()
}

pub fn load_config() -> Result<Config> {
    let config = Figment::new()
        .merge(Serialized::defaults(Config::default()))
        .merge(Env::raw())
        .merge(Serialized::defaults(Cli::parse()))
        .extract()?;
    Ok(config)
}
