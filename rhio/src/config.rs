use std::net::{AddrParseError, SocketAddr};
use std::path::{absolute, Path, PathBuf};
use std::str::FromStr;

use anyhow::{bail, Result};
use clap::Parser;
use figment::providers::{Env, Serialized};
use figment::Figment;
use p2panda_core::PublicKey;
use p2panda_net::config::{Config as NetworkConfig, NodeAddr};
use serde::{Deserialize, Serialize};
use url::Url;

use crate::topic_id::TopicId;
use crate::{BLOB_ANNOUNCE_TOPIC, FILE_SYSTEM_EVENT_TOPIC};

const DEFAULT_BLOBS_PATH: &str = "./blobs";

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Config {
    pub blobs_path: PathBuf,
    pub topics: Vec<TopicId>,
    #[serde(flatten)]
    pub network_config: NetworkConfig,
}

impl Default for Config {
    fn default() -> Self {
        let path = Path::new(DEFAULT_BLOBS_PATH);
        let topics = vec![
            TopicId::new_from_str(BLOB_ANNOUNCE_TOPIC),
            TopicId::new_from_str(FILE_SYSTEM_EVENT_TOPIC),
        ];

        let absolute_path = absolute(path).expect("to establish absolute path");
        Self {
            blobs_path: absolute_path,
            topics,
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
    direct_node_addresses: Option<Vec<NodeAddr>>,

    #[arg(short = 'k', long, value_name = "PATH")]
    #[serde(skip_serializing_if = "Option::is_none")]
    private_key: Option<PathBuf>,

    #[arg(short = 'b', long, value_name = "PATH")]
    #[serde(skip_serializing_if = "Option::is_none")]
    blobs_path: Option<PathBuf>,

    #[arg(short = 'r', long, value_name = "URL", num_args = 0.., value_parser = parse_url)]
    #[serde(skip_serializing_if = "Option::is_none")]
    relay_addresses: Option<Vec<Url>>,
}

fn parse_node_addr(value: &str) -> Result<NodeAddr> {
    let parts: Vec<&str> = value.split('|').collect();
    if parts.len() < 2 {
        bail!("node address needs to contain node id and at least one IP v4 or v6 address, separated with a pipe |");
    }

    let public_key = PublicKey::from_str(parts[0])?;
    let socket_addrs: Result<Vec<SocketAddr>, AddrParseError> = parts[1..]
        .iter()
        .map(|addr| SocketAddr::from_str(addr))
        .collect();

    Ok((public_key, socket_addrs?))
}

fn parse_url(value: &str) -> Result<Url> {
    Ok(value.parse()?)
}

pub fn load_config() -> Result<Config> {
    let mut config: Config = Figment::new()
        .merge(Serialized::defaults(Config::default()))
        .merge(Env::raw())
        .merge(Serialized::defaults(Cli::parse()))
        .extract()?;

    // Make blobs path absolute.
    let absolute_path = absolute(&config.blobs_path).expect("to establish absolute path");
    config.blobs_path = absolute_path;

    Ok(config)
}
