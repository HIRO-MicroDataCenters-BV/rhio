use std::sync::Arc;

use anyhow::{anyhow, Result};
use futures_lite::StreamExt;
use iroh_blobs::protocol::Closed;
use iroh_gossip::net::{Gossip, GOSSIP_ALPN};
use iroh_net::endpoint::TransportConfig;
use iroh_net::key::SecretKey;
use iroh_net::relay::RelayMode;
use iroh_net::util::SharedAbortingJoinHandle;
use iroh_net::{Endpoint, NodeId};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, error_span, warn, Instrument};

use crate::config::Config;
use crate::protocol::ProtocolMap;

const MAX_RPC_STREAMS: u32 = 1024;

const MAX_CONNECTIONS: u32 = 1024;

#[derive(Debug, Clone)]
pub struct Node {
    inner: Arc<NodeInner>,
    task: SharedAbortingJoinHandle<()>,
    protocols: Arc<ProtocolMap>,
}

#[derive(Debug)]
struct NodeInner {
    cancel_token: CancellationToken,
    config: Config,
    endpoint: Endpoint,
    gossip: Gossip,
    secret_key: SecretKey,
}

impl NodeInner {
    async fn spawn(self: Arc<Self>, protocols: Arc<ProtocolMap>) {
        let (ipv4, ipv6) = self.endpoint.bound_sockets();
        debug!(
            "listening at: {}{}",
            ipv4,
            ipv6.map(|addr| format!(" and {addr}")).unwrap_or_default()
        );

        let mut join_set = JoinSet::<Result<()>>::new();

        // Spawn a task that updates the gossip endpoints.
        {
            let inner = self.clone();
            join_set.spawn(async move {
                let mut stream = inner.endpoint.direct_addresses();
                while let Some(eps) = stream.next().await {
                    if let Err(err) = inner.gossip.update_direct_addresses(&eps) {
                        warn!("Failed to update direct addresses for gossip: {err:?}");
                    }
                }
                warn!("failed to retrieve local endpoints");
                Ok(())
            });
        }

        loop {
            tokio::select! {
                // Do not let tokio select futures randomly but with top-to-bottom priority.
                biased;
                // Exit loop when shutdown was signalled somewhere else.
                _ = self.cancel_token.cancelled() => {
                    break;
                },
                // Handle incoming p2p connections.
                Some(connecting) = self.endpoint.accept() => {
                    let protocols = protocols.clone();
                    join_set.spawn(async move {
                        handle_connection(connecting, protocols).await;
                        Ok(())
                    });
                },
                // Handle task terminations and quit on panics.
                res = join_set.join_next(), if !join_set.is_empty() => {
                    match res {
                        Some(Err(outer)) => {
                            if outer.is_panic() {
                                error!("Task panicked: {outer:?}");
                                break;
                            } else if outer.is_cancelled() {
                                debug!("Task cancelled: {outer:?}");
                            } else {
                                error!("Task failed: {outer:?}");
                                break;
                            }
                        }
                        Some(Ok(Err(inner))) => {
                            debug!("Task errored: {inner:?}");
                        }
                        _ => {}
                    }
                },
                else => break,
            }
        }

        self.shutdown(protocols).await;

        // Abort remaining tasks.
        join_set.shutdown().await;
    }

    async fn shutdown(&self, protocols: Arc<ProtocolMap>) {
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
            // Shutdown protocol handlers.
            protocols.shutdown(),
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
        let addr = endpoint.node_addr().await?;
        let node_id = endpoint.node_id();

        let gossip = Gossip::from_endpoint(endpoint.clone(), Default::default(), &addr.info);

        let mut protocols = ProtocolMap::default();
        protocols.insert(GOSSIP_ALPN, Arc::new(gossip.clone()));
        let protocols = Arc::new(protocols);

        let inner = Arc::new(NodeInner {
            cancel_token: CancellationToken::new(),
            config,
            endpoint,
            gossip,
            secret_key,
        });

        let fut = inner
            .clone()
            .spawn(protocols.clone())
            .instrument(error_span!("node", me=%node_id.fmt_short()));

        let task = tokio::task::spawn(fut);

        let node = Node {
            inner,
            task: task.into(),
            protocols,
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

async fn handle_connection(
    mut connecting: iroh_net::endpoint::Connecting,
    protocols: Arc<ProtocolMap>,
) {
    let alpn = match connecting.alpn().await {
        Ok(alpn) => alpn,
        Err(err) => {
            warn!("Ignoring connection: invalid handshake: {:?}", err);
            return;
        }
    };
    let Some(handler) = protocols.get(&alpn) else {
        warn!("Ignoring connection: unsupported ALPN protocol");
        return;
    };
    if let Err(err) = handler.accept(connecting).await {
        warn!("Handling incoming connection ended with error: {err}");
    }
}
