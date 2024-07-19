use std::net::{AddrParseError, SocketAddr};
use std::path::PathBuf;
use std::str::FromStr;

use anyhow::{bail, Result};
use clap::Parser;
use figment::providers::{Env, Serialized};
use figment::Figment;
use p2panda_core::PublicKey;
use p2panda_net::{Config as NetworkConfig, NodeAddress, RelayUrl};
use serde::{Deserialize, Serialize};

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

    #[arg(short = 'n', long, value_name = "\"NODE_ID|IP_ADDR|...\"", num_args = 0.., value_parser = parse_node_addr)]
    #[serde(skip_serializing_if = "Option::is_none")]
    direct_node_addresses: Option<Vec<NodeAddress>>,

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

fn parse_node_addr(value: &str) -> Result<NodeAddress> {
    let parts: Vec<&str> = value.split('|').collect();
    if parts.len() < 2 {
        bail!("node address needs to contain node id and at least one IP v4 or v6 address, separated with a pipe |");
    }

    let public_key = PublicKey::from_str(parts[0])?;
    let socket_addrs: Result<Vec<SocketAddr>, AddrParseError> = parts[1..]
        .iter()
        .map(|addr| SocketAddr::from_str(addr))
        .collect();

    Ok((public_key, socket_addrs?, None))
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
