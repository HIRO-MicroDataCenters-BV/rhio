use std::sync::Arc;

use crate::context::Context;
use crate::http::api::RhioApiImpl;
use crate::node::rhio::NodeOptions;

use crate::{blobs::store_from_config, nats::Nats, Node};

use anyhow::{anyhow, Context as AnyhowContext, Result};
use p2panda_blobs::Blobs as BlobsHandler;
use p2panda_core::{PrivateKey, PublicKey};
use p2panda_net::SyncConfiguration;
use p2panda_net::{NetworkBuilder, ResyncConfiguration};
use rhio_http_api::server::RhioHTTPServer;
use tokio::runtime::{Builder, Runtime};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;
use tracing::error;

use crate::blobs::{blobs_config, Blobs};
#[cfg(test)]
use crate::nats::client::fake::client::FakeNatsClient;
#[cfg(not(test))]
use crate::nats::client::nats::NatsClientImpl;
use crate::network::sync::RhioSyncProtocol;
use crate::network::Panda;
use crate::tracing::setup_tracing;
use figment::providers::Env;
use rhio_config::configuration::PRIVATE_KEY_ENV;
use rhio_config::configuration::{load_config, Config};
use rhio_core::load_private_key_from_file;

#[derive(Debug)]
pub struct ContextBuilder {
    config: Config,
    private_key: PrivateKey,
    public_key: PublicKey,
}

impl ContextBuilder {
    pub fn new(config: Config, private_key: PrivateKey) -> Self {
        setup_tracing(config.log_level.clone());
        ContextBuilder {
            public_key: private_key.public_key(),
            config,
            private_key,
        }
    }
    /// Load the configuration from the environment and initializes context builder
    pub fn from_cli() -> Result<Self> {
        let config = load_config()?;
        setup_tracing(config.log_level.clone());

        // Load the private key from either an environment variable _or_ a file specified in the
        // config. The environment variable takes priority.
        let private_key = match Env::var(PRIVATE_KEY_ENV) {
            Some(private_key_hex) => PrivateKey::try_from(&hex::decode(&private_key_hex)?[..])?,
            None => load_private_key_from_file(&config.node.private_key_path).context(format!(
                "could not load private key from file {}",
                config.node.private_key_path.display(),
            ))?,
        };

        let public_key = private_key.public_key();
        Ok(ContextBuilder {
            config,
            private_key,
            public_key,
        })
    }
    /// Attempts to build and start the `Context`.
    ///
    /// This method initializes the Rhio runtime and HTTP server runtime, then
    /// starts the Rhio node and HTTP server. The HTTP server is launched in a
    /// separate runtime to avoid blocking the Rhio runtime.
    ///
    /// # Returns
    ///
    /// * `Ok(Context)` - If the context is successfully built and started.
    /// * `Err(anyhow::Error)` - If there is an error during the initialization
    ///   process.
    ///
    /// # Errors
    ///
    /// This function will return an error if:
    /// * The Rhio tokio runtime cannot be built.
    /// * The Rhio node initialization fails.
    /// * The HTTP server tokio runtime cannot be built.
    /// * The HTTP server fails to start.
    ///
    pub fn try_build_and_start(&self) -> Result<Context> {
        let rhio_runtime = Builder::new_multi_thread()
            .enable_all()
            .thread_name("rhio")
            .build()
            .expect("Rhio tokio runtime");

        let node = rhio_runtime.block_on(async move {
            ContextBuilder::init_rhio_node(self.config.clone(), self.private_key.clone())
                .await
                .context("failed to initialize Rhio node")
        })?;

        // Launch HTTP server in separate runtime to not block rhio runtime.
        let http_runtime = Builder::new_multi_thread()
            .enable_io()
            .thread_name("http-server")
            .build()
            .expect("http server tokio runtime");
        let cancellation_token = CancellationToken::new();
        let http_handle = self.start_http_server(&http_runtime, cancellation_token.clone())?;

        Ok(Context::new(
            node,
            self.config.clone(),
            self.public_key,
            http_handle,
            http_runtime,
            cancellation_token,
            rhio_runtime,
        ))
    }

    async fn init_rhio_node(config: Config, private_key: PrivateKey) -> Result<Node> {
        // 1. Depends on the context - tests or prod, we configure corresponding nats client.
        #[cfg(not(test))]
        let nats_client = NatsClientImpl::new(config.nats.clone()).await?;
        #[cfg(test)]
        let nats_client = FakeNatsClient::new(config.nats.clone())?;

        // 2. Configure Nats consumer streams management
        let nats = Nats::new(nats_client).await?;

        // 3. Configure rhio peer-to-peer network.
        let (node_config, p2p_network_config) = Node::configure_p2p_network(&config).await?;

        let blob_store = store_from_config(&config).await?;

        let sync_protocol = RhioSyncProtocol::new(
            node_config.clone(),
            nats.clone(),
            blob_store.clone(),
            private_key.clone(),
        );

        let resync_config = config
            .node
            .protocol
            .map(|c| {
                ResyncConfiguration::new()
                    .poll_interval(c.poll_interval_seconds)
                    .interval(c.resync_interval_seconds)
            })
            .unwrap_or_default();

        let sync_config = SyncConfiguration::new(sync_protocol).resync(resync_config);

        let builder = NetworkBuilder::from_config(p2p_network_config)
            .private_key(private_key.clone())
            .sync(sync_config);

        let (watcher_tx, watcher_rx) = mpsc::channel(512);
        // 4. Configure and set up blob store and connection handlers for blob replication.
        let (network, blobs, watcher_rx) = if config.s3.is_some() {
            let (network, blobs_handler) =
                BlobsHandler::from_builder_with_config(builder, blob_store.clone(), blobs_config())
                    .await?;
            // 4.1. we also start a service which watches the S3 buckets for changes.
            let blobs = Blobs::new(blob_store.clone(), blobs_handler, watcher_tx);
            (network, Some(blobs), watcher_rx)
        } else {
            let network = builder.build().await?;
            (network, None, watcher_rx)
        };

        // 5. Move all networking logic into dedicated "p2panda" actor, dealing with p2p
        //    networking, data replication and gossipping.
        let node_id = network.node_id();
        let direct_addresses = network
            .direct_addresses()
            .await
            .ok_or_else(|| anyhow!("socket is not bind to any interface"))?;
        let panda = Panda::new(network);

        // 6. Creating rhio Node responsible for managing s3, nats and p2p network
        let options = NodeOptions {
            public_key: node_id,
            node_config,
            private_key: private_key.clone(),
            direct_addresses,
        };

        Node::new(nats, blobs, watcher_rx, panda, options).await
    }

    /// Starts the HTTP server with health endpoint.
    fn start_http_server(
        &self,
        runtime: &Runtime,
        cancellation_token: CancellationToken,
    ) -> Result<JoinHandle<Result<()>>> {
        let config = self.config.clone();
        let http_bind_port = self.config.node.http_bind_port;
        let api = Arc::new(RhioApiImpl::new(config).context("RhioAPIImpl initialization")?);
        let http_server = RhioHTTPServer::new(http_bind_port, api);
        Ok(runtime.spawn(async move {
            let result = http_server
                .run()
                .await
                .context("failed to start rhio http server")
                .inspect_err(|e| error!("http result {}", e));
            cancellation_token.cancel();
            result
        }))
    }
}
