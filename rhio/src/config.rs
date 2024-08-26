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

/// Default HTTP API endpoint for MinIO server.
pub const MINIO_ENDPOINT: &str = "http://localhost:9000";

/// Default S3 Region for MinIO blob store.
pub const MINIO_REGION: &str = "eu-west-2";

/// Default S3 bucket name for MinIO blob store.
pub const BUCKET_NAME: &str = "rhio";

#[derive(Clone, Default, Debug, Serialize, Deserialize)]
pub struct Config {
    pub blobs_dir: Option<PathBuf>,
    pub sync_dir: Option<PathBuf>,
    #[serde(flatten)]
    pub minio: MinioConfig,
    #[serde(flatten)]
    pub network_config: NetworkConfig,
}

#[derive(Parser, Serialize, Debug)]
#[command(
    name = "rhio",
    about = "Peer-to-peer message and blob streaming with MinIO and NATS Jetstream support",
    long_about = None,
    version
)]
struct Cli {
    /// Bind port of Node.
    #[arg(short = 'p', long, value_name = "PORT")]
    #[serde(skip_serializing_if = "Option::is_none")]
    bind_port: Option<u16>,

    /// Path to file containing hexadecimal-encoded Ed25519 private key.
    #[arg(short = 'k', long, value_name = "PATH")]
    #[serde(skip_serializing_if = "Option::is_none")]
    private_key: Option<PathBuf>,

    /// Path to file-system blob store to temporarily load blobs into when importing to MinIO
    /// database.
    // @TODO: This will be removed as soon as we've implemented a full MinIO storage backend. We
    // currently need it to generate all bao-tree hashes before moving the data further to MinIO.
    #[arg(short = 'b', long, value_name = "PATH")]
    #[serde(skip_serializing_if = "Option::is_none")]
    blobs_dir: Option<PathBuf>,

    /// MinIO database credentials.
    #[arg(short = 'c', long = "minio-credentials", value_name = "ACCESS_KEY:SECRET_KEY", value_parser = parse_s3_credentials)]
    #[serde(skip_serializing_if = "Option::is_none")]
    credentials: Option<Credentials>,

    /// MinIO database endpoint.
    #[arg(short = 'e', long = "minio-endpoint", value_name = "ENDPOINT")]
    #[serde(skip_serializing_if = "Option::is_none")]
    endpoint: Option<String>,

    /// MinIO bucket region string.
    #[arg(short = 'g', long = "minio-region", value_name = "REGION")]
    #[serde(skip_serializing_if = "Option::is_none")]
    region: Option<String>,

    /// MinIO bucket name for blob storage.
    #[arg(short = 'n', long = "minio-bucket-name", value_name = "NAME")]
    #[serde(skip_serializing_if = "Option::is_none")]
    bucket_name: Option<String>,
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

    let absolute_path = config
        .sync_dir
        .map(|path| absolute(path).expect("to establish absolute path"));
    config.sync_dir = absolute_path;

    Ok(config)
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MinioConfig {
    pub credentials: Option<Credentials>,
    pub bucket_name: String,
    pub endpoint: String,
    pub region: String,
}

impl Default for MinioConfig {
    fn default() -> Self {
        Self {
            credentials: None,
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
