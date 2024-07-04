use std::sync::Arc;

use anyhow::{anyhow, Result};
use clap::Parser;
use figment::providers::{Env, Serialized};
use figment::Figment;
use iroh_blobs::protocol::Closed;
use iroh_net::endpoint::TransportConfig;
use iroh_net::key::SecretKey;
use iroh_net::relay::RelayMode;
use iroh_net::util::SharedAbortingJoinHandle;
use iroh_net::{Endpoint, NodeId};
use serde::{Deserialize, Serialize};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{error_span, info, Instrument};
use tracing_subscriber::fmt::Layer;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

const DEFAULT_BIND_PORT: u16 = 4012;

const MAX_RPC_STREAMS: u32 = 1024;

const MAX_CONNECTIONS: u32 = 1024;

#[derive(Debug, Serialize, Deserialize)]
struct Config {
    bind_port: u16,
}

#[derive(Parser, Serialize, Debug)]
#[command(
    name = "rohi",
    about = "HIRO Blob Syncing Node",
    long_about = None,
    version
)]
struct Cli {
    #[arg(short = 'p', long, value_name = "PORT")]
    #[serde(skip_serializing_if = "Option::is_none")]
    bind_port: Option<u16>,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            bind_port: DEFAULT_BIND_PORT,
        }
    }
}

fn setup_logging() {
    tracing_subscriber::registry()
        .with(Layer::default())
        .with(EnvFilter::from_default_env())
        .try_init()
        .ok();
}

fn load_config() -> Result<Config> {
    let cli = Cli::parse();

    let figment = Figment::from(Serialized::defaults(Config::default()));

    let config = figment
        .merge(Env::raw())
        .merge(Serialized::defaults(cli))
        .extract()?;

    Ok(config)
}

#[derive(Debug, Clone)]
struct Node {
    inner: Arc<NodeInner>,
    task: SharedAbortingJoinHandle<()>,
}

#[derive(Debug)]
struct NodeInner {
    cancel_token: CancellationToken,
    config: Config,
    endpoint: Endpoint,
    secret_key: SecretKey,
}

impl NodeInner {
    async fn spawn(self: Arc<Self>) {
        let mut join_set = JoinSet::<Result<()>>::new();

        join_set.spawn(async { Ok(()) });

        loop {
            tokio::select! {
                // Do not let tokio select futures randomly but with top-to-bottom priority
                biased;
                _ = self.cancel_token.cancelled() => {
                    break;
                },
            }
        }

        self.shutdown().await;

        // Abort remaining tasks.
        join_set.shutdown().await;
    }

    async fn shutdown(&self) {
        let error_code = Closed::ProviderTerminating;

        // We ignore all errors during shutdown.
        let _ = tokio::join!(
            // Close the endpoint.
            // Closing the Endpoint is the equivalent of calling Connection::close on all
            // connections: Operations will immediately fail with ConnectionError::LocallyClosed.
            // All streams are interrupted, this is not graceful.
            self.endpoint
                .clone()
                .close(error_code.into(), error_code.reason()),
        );
    }
}

impl Node {
    pub async fn spawn(config: Config) -> Result<Self> {
        let secret_key = SecretKey::generate();
        let mut transport_config = TransportConfig::default();
        transport_config
            .max_concurrent_bidi_streams(MAX_RPC_STREAMS.into())
            .max_concurrent_uni_streams(0u32.into());

        let builder = Endpoint::builder()
            .transport_config(transport_config)
            .secret_key(secret_key.clone())
            .relay_mode(RelayMode::Disabled)
            .concurrent_connections(MAX_CONNECTIONS);

        let endpoint = builder.bind(config.bind_port).await?;
        let node_id = endpoint.node_id();

        let inner = Arc::new(NodeInner {
            cancel_token: CancellationToken::new(),
            config,
            endpoint,
            secret_key,
        });

        let fut = inner
            .clone()
            .spawn()
            .instrument(error_span!("node", me=%node_id.fmt_short()));

        let task = tokio::task::spawn(fut);

        let node = Node {
            inner,
            task: task.into(),
        };

        Ok(node)
    }

    /// Returns the public key of the node.
    pub fn node_id(&self) -> NodeId {
        self.inner.secret_key.public()
    }

    pub async fn shutdown(self) -> Result<()> {
        // Trigger shutdown of the main run task by activating the cancel token.
        self.inner.cancel_token.cancel();

        // Wait for the main task to terminate.
        self.task.await.map_err(|err| anyhow!(err))?;

        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    setup_logging();

    let config = load_config()?;
    let node = Node::spawn(config).await?;
    info!("Node ID: {}", node.node_id());

    tokio::select! {
        _ = tokio::signal::ctrl_c() => (),
    }

    node.shutdown().await?;

    Ok(())
}
