use std::net::{AddrParseError, SocketAddr};
use std::path::PathBuf;
use std::str::FromStr;

use anyhow::{bail, Result};
use clap::Parser;
use figment::providers::{Env, Serialized};
use figment::Figment;
use iroh_net::NodeId;
use p2panda_core::PublicKey;
use p2panda_net::config::{Config, NodeAddr};
use serde::{Deserialize, Serialize};

const DEFAULT_BIND_PORT: u16 = 4012;

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

}

fn parse_node_addr(value: &str) -> Result<NodeAddr> {
    let parts: Vec<&str> = value.split("|").collect();
    if parts.len() < 2 {
        bail!("node address needs to contain node id and at least one IP v4 or v6 address, separated with a pipe |");
    }

    let node_id = NodeId::from_str(&parts[0])?;
    let public_key = PublicKey::from_bytes(node_id.as_bytes())?;
    let socket_addrs: Result<Vec<SocketAddr>, AddrParseError> = parts[1..]
        .iter()
        .map(|addr| SocketAddr::from_str(addr))
        .collect();

    Ok((public_key, socket_addrs?))
}

pub fn load_config() -> Result<Config> {
    let config = Figment::new()
        .merge(Serialized::defaults(Config::default()))
        .merge(Env::raw())
        .merge(Serialized::defaults(Cli::parse()))
        .extract()?;

    println!("{config:?}");

    Ok(config)
}
