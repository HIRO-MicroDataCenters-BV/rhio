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
    pub bucket_name: String,
    pub bucket_address: BucketAddress,
    pub credentials: Credentials,
    pub blobs_dir: Option<PathBuf>,
    pub import_path: Option<ImportPath>,
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

        let credentials = Credentials::default()
            .unwrap_or(Credentials::anonymous().expect("method can never fail"));
        Self {
            bucket_name: BUCKET_NAME.to_string(),
            bucket_address: BucketAddress::default(),
            credentials: credentials,
            blobs_dir: None,
            sync_dir: None,
            import_path: None,
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
    ticket: Option<Vec<Ticket>>,

    #[arg(short = 'k', long, value_name = "PATH")]
    #[serde(skip_serializing_if = "Option::is_none")]
    private_key: Option<PathBuf>,

    #[arg(short = 's', long, value_name = "PATH")]
    #[serde(skip_serializing_if = "Option::is_none")]
    sync_dir: Option<PathBuf>,

    #[arg(short = 'b', long, value_name = "PATH")]
    #[serde(skip_serializing_if = "Option::is_none")]
    blobs_dir: Option<PathBuf>,

    #[arg(short = 'i', long, value_name = "PATH | URL", value_parser=parse_import_path)]
    #[serde(skip_serializing_if = "Option::is_none")]
    import_path: Option<ImportPath>,

    #[arg(short = 'c', long, value_name = "ACCESS_KEY:SECRET_KEY", value_parser = parse_s3_credentials)]
    #[serde(skip_serializing_if = "Option::is_none")]
    credentials: Option<Credentials>,

    #[arg(short = 'a', long, value_name = "ENDPOINT:REGION", value_parser = parse_url)]
    #[serde(skip_serializing_if = "Option::is_none")]
    bucket_address: Option<BucketAddress>,

    #[arg(short = 'n', long, value_name = "NAME", value_parser = parse_url)]
    #[serde(skip_serializing_if = "Option::is_none")]
    bucket_name: Option<String>,

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

pub fn parse_bucket_address(value: &str) -> Result<BucketAddress> {
    let mut value_iter = value.split(":");

    let endpoint = value_iter
        .next()
        .ok_or(anyhow::anyhow!("no endpoint provided"))?
        .to_string();

    let region = value_iter.next().unwrap_or(MINIO_REGION).to_string();

    Ok(BucketAddress { endpoint, region })
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
pub struct MinioCredentials {
    pub access_key: String,
    pub secret_key: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BucketAddress {
    pub region: String,
    pub endpoint: String,
}

impl Default for BucketAddress {
    fn default() -> Self {
        Self {
            region: MINIO_REGION.to_string(),
            endpoint: MINIO_ENDPOINT.to_string(),
        }
    }
}

type Url = String;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ImportPath {
    File(PathBuf),
    Url(Url),
}

impl Display for ImportPath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ImportPath::File(path) => write!(f, "{}", path.to_string_lossy()),
            ImportPath::Url(url) => write!(f, "{url}"),
        }
    }
}
