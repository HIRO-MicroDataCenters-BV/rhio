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

/// Default rhio network id.
const DEFAULT_NETWORK_ID: &str = "rhio-default-network-1";

/// Default HTTP API endpoint for MinIO server.
pub const S3_ENDPOINT: &str = "http://localhost:9000";

/// Default S3 Region for MinIO blob store.
pub const S3_REGION: &str = "eu-west-2";

/// Default endpoint for NATS server.
pub const NATS_ENDPOINT: &str = "localhost:4222";

#[derive(Clone, Default, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Config {
    pub s3: S3Config,
    pub nats: NatsConfig,
    #[serde(flatten)]
    pub node: NodeConfig,
    pub log_level: Option<String>,
    pub publish: Option<PublishConfig>,
    pub subscribe: Option<SubscribeConfig>,
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

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct S3Config {
    // @TODO(adz): We probably want to load this from some secure store / file instead?
    // See related issue: https://github.com/HIRO-MicroDataCenters-BV/rhio/issues/59
    pub credentials: Option<Credentials>,
    #[deprecated]
    pub bucket_name: String,
    pub endpoint: String,
    pub region: String,
}

impl Default for S3Config {
    fn default() -> Self {
        Self {
            credentials: None,
            bucket_name: "".to_string(),
            endpoint: S3_ENDPOINT.to_string(),
            region: S3_REGION.to_string(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NatsConfig {
    pub endpoint: String,
    pub credentials: Option<NatsCredentials>,
}

impl Default for NatsConfig {
    fn default() -> Self {
        Self {
            endpoint: NATS_ENDPOINT.to_string(),
            credentials: None,
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct NatsCredentials {
    pub nkey: Option<String>,
    pub username: Option<String>,
    pub password: Option<String>,
    pub token: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NodeConfig {
    pub bind_port: u16,
    #[serde(rename = "nodes")]
    pub known_nodes: Vec<KnownNode>,
    #[serde(rename = "private_key_path")]
    pub private_key: Option<PathBuf>,
    pub network_id: String,
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            bind_port: DEFAULT_BIND_PORT,
            known_nodes: vec![],
            private_key: None,
            network_id: DEFAULT_NETWORK_ID.to_string(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct KnownNode {
    pub public_key: PublicKey,
    #[serde(rename = "endpoints")]
    pub direct_addresses: Vec<SocketAddr>,
}

#[deprecated]
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct StreamConfig {
    pub nats_stream_name: String,
    pub nats_filter_subject: Option<String>,
    pub external_topic: String,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct PublishConfig {
    pub s3_buckets: Vec<Bucket>,
    pub nats_subjects: Vec<Subject>,
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct SubscribeConfig {
    pub s3_buckets: Vec<ScopedBucket>,
    pub nats_subjects: Vec<ScopedSubject>,
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use figment::providers::{Format, Serialized, Toml};
    use figment::Figment;
    use s3::creds::Credentials;

    use crate::config::{
        KnownNode, NatsConfig, NatsCredentials, NodeConfig, PublishConfig, S3Config,
        SubscribeConfig,
    };

    use super::Config;

    #[test]
    fn parse_toml_file() {
        figment::Jail::expect_with(|jail| {
            jail.create_file(
                "config.toml",
                r#"
bind_port = 1112
private_key_path = "/usr/app/rhio/private.key"
network_id = "rhio-default-network-1"

[s3]
endpoint = "http://minio.svc.kubernetes.local"
region = "eu-central-1"

[s3.credentials]
access_key = "access_key_test"
secret_key = "secret_key_test"

[nats]
endpoint = "http://nats.svc.kubernetes.local"

[nats.credentials]
username = "username_test"
password = "password_test"

[[nodes]]
public_key = "6ee91c497d577b5c21ab53212c194b56779addd8088d8b850ece447c8844fe8a"
endpoints = [
  "192.168.178.100:2022",
  "[2a02:8109:9c9a:4200:eb13:7c0a:4201:8128]:2023",
]

[publish]
s3_buckets = [
  "local_bucket_1",
  "local_bucket_2",
]
nats_subjects = [
  "workload.berlin.energy",
  "workload.rotterdam.energy",
]

[subscribe]
s3_buckets = [
  "6ee91c497d577b5c21ab53212c194b56779addd8088d8b850ece447c8844fe8a/remote_bucket_1",
  "6ee91c497d577b5c21ab53212c194b56779addd8088d8b850ece447c8844fe8a/remote_bucket_2",
]
nats_subjects = [
  "6ee91c497d577b5c21ab53212c194b56779addd8088d8b850ece447c8844fe8a/workload.*.energy",
  "6ee91c497d577b5c21ab53212c194b56779addd8088d8b850ece447c8844fe8a/data.thehague.meta",
]
            "#,
            )?;

            let config: Config = Figment::from(Serialized::defaults(Config::default()))
                .merge(Toml::file("config.toml"))
                .extract()
                .unwrap();

            assert_eq!(
                config,
                Config {
                    s3: S3Config {
                        credentials: Some(Credentials {
                            access_key: Some("access_key_test".into()),
                            secret_key: Some("secret_key_test".into()),
                            security_token: None,
                            session_token: None,
                            expiration: None
                        }),
                        bucket_name: String::new(),
                        endpoint: "http://minio.svc.kubernetes.local".into(),
                        region: "eu-central-1".into(),
                    },
                    nats: NatsConfig {
                        endpoint: "http://nats.svc.kubernetes.local".into(),
                        credentials: Some(NatsCredentials {
                            nkey: None,
                            username: Some("username_test".into()),
                            password: Some("password_test".into()),
                            token: None
                        }),
                    },
                    node: NodeConfig {
                        bind_port: 1112,
                        known_nodes: vec![KnownNode {
                            public_key:
                                "6ee91c497d577b5c21ab53212c194b56779addd8088d8b850ece447c8844fe8a"
                                    .parse()
                                    .unwrap(),
                            direct_addresses: vec![
                                "192.168.178.100:2022".parse().unwrap(),
                                "[2a02:8109:9c9a:4200:eb13:7c0a:4201:8128]:2023"
                                    .parse()
                                    .unwrap(),
                            ],
                        }],
                        private_key: Some(PathBuf::new().join("/usr/app/rhio/private.key")),
                        network_id: "rhio-default-network-1".into(),
                    },
                    log_level: None,
                    publish: Some(PublishConfig {
                        s3_buckets: vec!["local_bucket_1".into(), "local_bucket_2".into()],
                        nats_subjects: vec![
                            "workload.berlin.energy".parse().unwrap(),
                            "workload.rotterdam.energy".parse().unwrap(),
                        ],
                    }),
                    subscribe: Some(SubscribeConfig {
                        s3_buckets: vec![
                            "6ee91c497d577b5c21ab53212c194b56779addd8088d8b850ece447c8844fe8a/remote_bucket_1"
                                .parse()
                                .unwrap(),
                            "6ee91c497d577b5c21ab53212c194b56779addd8088d8b850ece447c8844fe8a/remote_bucket_2"
                                .parse()
                                .unwrap(),
                        ],
                        nats_subjects: vec![
                            "6ee91c497d577b5c21ab53212c194b56779addd8088d8b850ece447c8844fe8a/workload.*.energy".parse().unwrap(),
                            "6ee91c497d577b5c21ab53212c194b56779addd8088d8b850ece447c8844fe8a/data.thehague.meta".parse().unwrap(),
                        ],
                    }),
                    streams: None
                }
            );

            Ok(())
        });
    }
}
