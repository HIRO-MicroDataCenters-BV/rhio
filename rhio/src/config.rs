use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::{bail, Result};
use clap::Parser;
use directories::ProjectDirs;
use figment::providers::{Env, Format, Serialized, Toml};
use figment::Figment;
use p2panda_core::PublicKey;
use rhio_core::{Bucket, ScopedBucket, ScopedSubject, Subject};
use s3::creds::Credentials;
use serde::{Deserialize, Serialize};

/// Default file name of config.
const CONFIG_FILE_NAME: &str = "config.toml";

/// Default rhio port.
const DEFAULT_BIND_PORT: u16 = 9102;

/// Default HTTP API endpoint for MinIO server.
pub const MINIO_ENDPOINT: &str = "http://localhost:9000";

/// Default S3 Region for MinIO blob store.
pub const MINIO_REGION: &str = "eu-west-2";

/// Default S3 bucket name for MinIO blob store.
#[deprecated]
pub const BUCKET_NAME: &str = "rhio";

/// Default endpoint for NATS server.
pub const NATS_ENDPOINT: &str = "localhost:4222";

#[derive(Clone, Default, Debug, Serialize, Deserialize)]
pub struct Config {
    pub blobs_dir: Option<PathBuf>,
    #[serde(rename = "s3")]
    pub minio: MinioConfig,
    pub nats: NatsConfig,
    #[serde(flatten)]
    pub node: NodeConfig,
    pub log_level: Option<String>,
    pub publish: Option<Vec<PublishConfig>>,
    #[deprecated]
    pub streams: Option<Vec<StreamConfig>>,
}

#[derive(Parser, Serialize, Debug)]
#[command(
    name = "rhio",
    about = "Peer-to-peer message and blob streaming with MinIO and NATS JetStream support",
    long_about = None,
    version
)]
struct Cli {
    /// Path to "config.toml" file for further configuration.
    ///
    /// When not set the program will try to find a `config.toml` file in the same folder the
    /// program is executed in and otherwise in the regarding operation systems XDG config
    /// directory ("$HOME/.config/rhio/config.toml" on Linux).
    #[arg(short = 'c', long, value_name = "PATH")]
    #[serde(skip_serializing_if = "Option::is_none")]
    config: Option<PathBuf>,

    /// Bind port of rhio node.
    #[arg(short = 'p', long, value_name = "PORT")]
    #[serde(skip_serializing_if = "Option::is_none")]
    bind_port: Option<u16>,

    /// Path to file containing hexadecimal-encoded Ed25519 private key.
    #[arg(short = 'k', long, value_name = "PATH")]
    #[serde(skip_serializing_if = "Option::is_none")]
    private_key: Option<PathBuf>,

    /// Path to file-system blob store to temporarily load blobs into when importing to MinIO
    /// database.
    ///
    /// WARNING: When left empty, an in-memory blob store is used instead which might lead to data
    /// corruption as blob hashes are not kept between restarts. Use the in-memory store only for
    /// testing purposes.
    // @TODO: This will be removed as soon as we've implemented a full MinIO storage backend. We
    // currently need it to generate all bao-tree hashes before moving the data further to MinIO.
    // See related issue: https://github.com/HIRO-MicroDataCenters-BV/rhio/issues/51
    #[deprecated]
    #[arg(short = 'b', long, value_name = "PATH")]
    #[serde(skip_serializing_if = "Option::is_none")]
    blobs_dir: Option<PathBuf>,

    /// Set log verbosity. Use this for learning more about how your node behaves or for debugging.
    ///
    /// Possible log levels are: ERROR, WARN, INFO, DEBUG, TRACE. They are scoped to "rhio" by
    /// default.
    ///
    /// If you want to adjust the scope for deeper inspection use a filter value, for example
    /// "=TRACE" for logging _everything_ or "rhio=INFO,async_nats=DEBUG" etc.
    #[arg(short = 'l', long, value_name = "LEVEL")]
    #[serde(skip_serializing_if = "Option::is_none")]
    log_level: Option<String>,
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
    // @TODO(adz): We probably want to load this from some secure store / file instead?
    // See related issue: https://github.com/HIRO-MicroDataCenters-BV/rhio/issues/59
    pub credentials: Option<Credentials>,
    #[deprecated]
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
    pub credentials: Option<NatsAuth>,
}

impl Default for NatsConfig {
    fn default() -> Self {
        Self {
            endpoint: NATS_ENDPOINT.to_string(),
            credentials: None,
        }
    }
}

#[derive(Clone, Default, Debug, Serialize, Deserialize)]
pub struct NatsAuth {
    pub nkey: Option<String>,
    pub username: Option<String>,
    pub password: Option<String>,
    pub token: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NodeConfig {
    pub bind_port: u16,
    #[serde(rename = "nodes")]
    pub known_nodes: Vec<KnownNode>,
    #[serde(rename = "private_key_path")]
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
    #[serde(rename = "endpoints")]
    pub direct_addresses: Vec<SocketAddr>,
}

#[deprecated]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StreamConfig {
    pub nats_stream_name: String,
    pub nats_filter_subject: Option<String>,
    pub external_topic: String,
}

#[derive(Clone, Default, Debug, Serialize, Deserialize)]
pub struct PublishConfig {
    pub s3_buckets: Vec<Bucket>,
    pub nats_subjects: Vec<Subject>,
}

#[derive(Clone, Default, Debug, Serialize, Deserialize)]
pub struct SubscribeConfig {
    pub s3_buckets: Vec<ScopedBucket>,
    pub nats_subjects: Vec<ScopedSubject>,
}
