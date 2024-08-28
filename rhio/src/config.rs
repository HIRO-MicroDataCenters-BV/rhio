use std::fmt::Display;
use std::net::SocketAddr;
use std::path::{absolute, PathBuf};
use std::str::FromStr;

use anyhow::{bail, Result};
use clap::Parser;
use directories::ProjectDirs;
use figment::providers::{Env, Format, Serialized, Toml};
use figment::Figment;
use p2panda_core::PublicKey;
use p2panda_net::config::DEFAULT_BIND_PORT;
use s3::creds::Credentials;
use serde::{Deserialize, Serialize};

/// Default file name of config.
const CONFIG_FILE_NAME: &str = "config.toml";

/// Default HTTP API endpoint for MinIO server.
pub const MINIO_ENDPOINT: &str = "http://localhost:9000";

/// Default S3 Region for MinIO blob store.
pub const MINIO_REGION: &str = "eu-west-2";

/// Default S3 bucket name for MinIO blob store.
pub const BUCKET_NAME: &str = "rhio";

/// Default endpoint for NATS server.
pub const NATS_ENDPOINT: &str = "localhost:4222";

#[derive(Clone, Default, Debug, Serialize, Deserialize)]
pub struct Config {
    // @TODO: Remove this as soon as we've implemented full MinIO storage.
    pub blobs_dir: Option<PathBuf>,
    pub minio: MinioConfig,
    pub nats: NatsConfig,
    #[serde(flatten)]
    pub node: NodeConfig,
    pub log_level: Option<String>,
}

#[derive(Parser, Serialize, Debug)]
#[command(
    name = "rhio",
    about = "Peer-to-peer message and blob streaming with MinIO and NATS Jetstream support",
    long_about = None,
    version
)]
struct Cli {
    /// Path to an optional "config.toml" file for further configuration.
    ///
    /// When not set the program will try to find a `config.toml` file in the same folder the
    /// program is executed in and otherwise in the regarding operation systems XDG config
    /// directory ("$HOME/.config/rhio/config.toml" on Linux).
    #[arg(short = 'c', long, value_name = "PATH")]
    #[serde(skip_serializing_if = "Option::is_none")]
    config: Option<PathBuf>,

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

    /// Set log verbosity. Use this for learning more about how your node behaves or for debugging.
    ///
    /// Possible log levels are: ERROR, WARN, INFO, DEBUG, TRACE. They are scoped to "rhio" by
    /// default.
    ///
    /// If you want to adjust the scope for deeper inspection use a filter value, for example
    /// "=TRACE" for logging _everything_ or "rhio=INFO,async-nats=DEBUG" etc.
    #[arg(short = 'l', long, value_name = "LEVEL")]
    #[serde(skip_serializing_if = "Option::is_none")]
    log_level: Option<String>,
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

/// Get configuration from 1. .toml file, 2. environment variables and 3. command line arguments
/// (in that order, meaning that later configuration sources take precedence over the earlier
/// ones).
///
/// Returns a partly unchecked configuration object which results from all of these sources.
pub fn load_config() -> Result<Config> {
    // Parse command line arguments and CONFIG environment variable first to get optional config
    // file path
    let cli = Cli::parse();

    // Determine if a config file path was provided or if we should look for it in common locations
    let config_file_path: Option<PathBuf> = match &cli.config {
        Some(path) => {
            if !path.exists() {
                bail!("config file '{}' does not exist", path.display());
            }

            Some(path.clone())
        }
        None => try_determine_config_file_path(),
    };

    let mut figment = Figment::from(Serialized::defaults(Config::default()));

    if let Some(path) = &config_file_path {
        figment = figment.merge(Toml::file(path));
    }

    let config: Config = figment
        .merge(Env::raw())
        .merge(Serialized::defaults(cli))
        .extract()?;

    Ok(config)
}

fn try_determine_config_file_path() -> Option<PathBuf> {
    // Find config file in current folder
    let mut current_dir = std::env::current_dir().expect("could not determine current directory");
    current_dir.push(CONFIG_FILE_NAME);

    // Find config file in XDG config folder
    let mut xdg_config_dir: PathBuf = ProjectDirs::from("", "", "rhio")
        .expect("could not determine valid config directory path from operating system")
        .config_dir()
        .to_path_buf();
    xdg_config_dir.push(CONFIG_FILE_NAME);

    [current_dir, xdg_config_dir]
        .iter()
        .find(|path| path.exists())
        .cloned()
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

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NatsConfig {
    pub endpoint: String,
}

impl Default for NatsConfig {
    fn default() -> Self {
        Self {
            endpoint: NATS_ENDPOINT.to_string(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NodeConfig {
    pub bind_port: u16,
    #[serde(rename = "nodes")]
    pub known_nodes: Vec<KnownNode>,
    pub private_key: Option<PathBuf>,
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            bind_port: DEFAULT_BIND_PORT,
            known_nodes: vec![],
            private_key: None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KnownNode {
    pub public_key: PublicKey,
    pub direct_addresses: Vec<SocketAddr>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ImportPath {
    File(PathBuf),
    Url(String),
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
