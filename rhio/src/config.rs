use std::fmt::Display;
use std::path::{absolute, PathBuf};
use std::str::FromStr;

use anyhow::Result;
use clap::Parser;
use figment::providers::{Env, Serialized};
use figment::Figment;
use p2panda_net::{Config as NetworkConfig, RelayUrl};
use s3::creds::Credentials;
use serde::{Deserialize, Serialize};

use crate::ticket::Ticket;
use crate::{BUCKET_NAME, MINIO_ENDPOINT, MINIO_REGION};

// Use iroh's staging relay node for testing
const DEFAULT_RELAY_URL: &str = "https://staging-euw1-1.relay.iroh.network";

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Config {
    pub blobs_dir: Option<PathBuf>,
    pub sync_dir: Option<PathBuf>,
    #[serde(flatten)]
    pub minio: MinioConfig,
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
            minio: MinioConfig::default(),
            blobs_dir: None,
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
    /// node bind port
    #[arg(short = 'p', long, value_name = "PORT")]
    #[serde(skip_serializing_if = "Option::is_none")]
    bind_port: Option<u16>,

    /// connection ticket string
    #[arg(short = 't', long, value_name = "TICKET", num_args = 0.., value_parser = parse_ticket)]
    #[serde(skip_serializing_if = "Option::is_none")]
    ticket: Option<Vec<Ticket>>,

    /// path to private key
    #[arg(short = 'k', long, value_name = "PATH")]
    #[serde(skip_serializing_if = "Option::is_none")]
    private_key: Option<PathBuf>,

    /// path to sync directory (for use with example/sync)
    #[arg(short = 's', long, value_name = "PATH")]
    #[serde(skip_serializing_if = "Option::is_none")]
    sync_dir: Option<PathBuf>,

    /// path to blob store and database
    #[arg(short = 'b', long, value_name = "PATH")]
    #[serde(skip_serializing_if = "Option::is_none")]
    blobs_dir: Option<PathBuf>,

    /// minio credentials
    #[arg(short = 'c', long = "minio-credentials", value_name = "ACCESS_KEY:SECRET_KEY", value_parser = parse_s3_credentials)]
    #[serde(skip_serializing_if = "Option::is_none")]
    credentials: Option<Credentials>,

    /// minio bucket endpoint string
    #[arg(short = 'e', long = "minio-endpoint", value_name = "ENDPOINT")]
    #[serde(skip_serializing_if = "Option::is_none")]
    endpoint: Option<String>,

    /// minio bucket region string
    #[arg(short = 'g', long = "minio-region", value_name = "REGION")]
    #[serde(skip_serializing_if = "Option::is_none")]
    region: Option<String>,

    /// minio bucket name
    #[arg(short = 'n', long = "minio-bucket-name", value_name = "NAME")]
    #[serde(skip_serializing_if = "Option::is_none")]
    bucket_name: Option<String>,

    /// relay addresses
    #[arg(short = 'r', long, value_name = "URL", value_parser = parse_url)]
    #[serde(skip_serializing_if = "Option::is_none")]
    relay: Option<RelayUrl>,
}

pub fn parse_ticket(value: &str) -> Result<Ticket> {
    let ticket = Ticket::from_str(value)?;
    Ok(ticket)
}

pub fn parse_url(value: &str) -> Result<RelayUrl> {
    value.parse()
}

pub fn parse_import_path(value: &str) -> Result<ImportPath> {
    if value.starts_with("http:") || value.starts_with("https:") {
        Ok(ImportPath::Url(value.to_string()))
    } else {
        let absolute_path = absolute(PathBuf::from(value))?;
        Ok(ImportPath::File(absolute_path))
    }
}

pub fn parse_s3_credentials(value: &str) -> Result<Credentials> {
    let mut value_iter = value.split(":");
    let access_key = value_iter
        .next()
        .ok_or(anyhow::anyhow!("no access key provided"))?;

    let secret_key = value_iter
        .next()
        .ok_or(anyhow::anyhow!("no secret key provided"))?;

    let credentials = Credentials::new(Some(access_key), Some(secret_key), None, None, None)?;
    Ok(credentials)
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

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MinioConfig {
    pub credentials: Credentials,
    pub bucket_name: String,
    pub endpoint: String,
    pub region: String,
}

impl Default for MinioConfig {
    fn default() -> Self {
        let credentials = Credentials::default()
            .unwrap_or(Credentials::anonymous().expect("method can never fail"));

        Self {
            credentials,
            bucket_name: BUCKET_NAME.to_string(),
            endpoint: MINIO_ENDPOINT.to_string(),
            region: MINIO_REGION.to_string(),
        }
    }
}

type Url = String;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ImportPath {
    File(PathBuf),
    Url(Url),
}

impl FromStr for ImportPath {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        parse_import_path(s)
    }
}

impl Display for ImportPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ImportPath::File(path) => write!(f, "{}", path.to_string_lossy()),
            ImportPath::Url(url) => write!(f, "{url}"),
        }
    }
}
