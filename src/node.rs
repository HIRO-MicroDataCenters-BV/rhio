use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use futures_lite::{Stream, StreamExt};
use iroh_blobs::downloader::Downloader;
use iroh_blobs::get::db::DownloadProgress;
use iroh_blobs::protocol::{Closed, ALPN as BLOBS_ALPN};
use iroh_blobs::provider::AddProgress;
use iroh_blobs::store::mem::Store as MemoryStore;
use iroh_blobs::store::Store;
use iroh_blobs::util::progress::{FlumeProgressSender, ProgressSender};
use iroh_blobs::{BlobFormat, Hash, HashAndFormat};
use iroh_gossip::net::{Gossip, GOSSIP_ALPN};
use iroh_net::endpoint::{Connection, DirectAddr, TransportConfig};
use iroh_net::key::SecretKey;
use iroh_net::relay::RelayMode;
use iroh_net::util::SharedAbortingJoinHandle;
use iroh_net::{Endpoint, NodeAddr, NodeId};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tokio_util::task::LocalPoolHandle;
use tracing::{debug, error, error_span, warn, Instrument};

use crate::blobs::{add_from_path, download_queued, BlobAddPathResponse, BlobDownloadResponse};
use crate::config::Config;
use crate::protocol::{BlobsProtocol, BubuProtocol, ProtocolMap, BUBU_ALPN};

const MAX_RPC_STREAMS: u32 = 1024;
const MAX_CONNECTIONS: u32 = 1024;

/// How long we wait at most for some endpoints to be discovered.
const ENDPOINT_WAIT: Duration = Duration::from_secs(5);

#[derive(Debug, Clone)]
pub struct Node<D> {
    inner: Arc<NodeInner<D>>,
    task: SharedAbortingJoinHandle<()>,
    protocols: Arc<ProtocolMap>,
}

#[derive(Debug)]
struct NodeInner<D> {
    cancel_token: CancellationToken,
    config: Config,
    db: D,
    endpoint: Endpoint,
    gossip: Gossip,
    secret_key: SecretKey,
    pool_handle: LocalPoolHandle,
    downloader: Downloader,
}

impl<D> NodeInner<D>
where
    D: Store,
{
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

impl Node<MemoryStore> {
    pub async fn spawn(config: Config) -> Result<Self> {
        let secret_key = SecretKey::generate();
        let mut transport_config = TransportConfig::default();
        transport_config
            .max_concurrent_bidi_streams(MAX_RPC_STREAMS.into())
            .max_concurrent_uni_streams(0u32.into());

        let builder = Endpoint::builder()
            .transport_config(transport_config)
            .secret_key(secret_key.clone())
            .relay_mode(RelayMode::Default)
            .concurrent_connections(MAX_CONNECTIONS);

        let endpoint = builder.bind(config.bind_port).await?;
        let addr = endpoint.node_addr().await?;
        let node_id = endpoint.node_id();

        for node_addr in &config.direct_node_addresses {
            endpoint.add_node_addr(node_addr.clone())?;
        }

        let gossip = Gossip::from_endpoint(endpoint.clone(), Default::default(), &addr.info);

        for node_addr in &config.direct_node_addresses {
            endpoint.add_node_addr(node_addr.clone())?;
        }

        let pool_handle = LocalPoolHandle::new(num_cpus::get());
        let db = MemoryStore::new();
        let downloader = Downloader::new(db.clone(), endpoint.clone(), pool_handle.clone());

        let mut protocols = ProtocolMap::default();
        protocols.insert(GOSSIP_ALPN, Arc::new(gossip.clone()));
        protocols.insert(
            BLOBS_ALPN,
            Arc::new(BlobsProtocol::new(db.clone(), pool_handle.clone())),
        );
        protocols.insert(BUBU_ALPN, Arc::new(BubuProtocol {}));
        let protocols = Arc::new(protocols);

        let inner = Arc::new(NodeInner {
            cancel_token: CancellationToken::new(),
            config,
            db,
            endpoint: endpoint.clone(),
            gossip,
            secret_key,
            pool_handle,
            downloader,
        });

        let fut = inner
            .clone()
            .spawn(protocols.clone())
            .instrument(error_span!("node", me=%node_id.fmt_short()));

        let task = tokio::task::spawn(fut);

        let node = Node {
            inner,
            task: task.into(),
            protocols: protocols.clone(),
        };

        // Update the endpoint with our ALPNs.
        let alpns = protocols
            .alpns()
            .map(|alpn| alpn.to_vec())
            .collect::<Vec<_>>();
        if let Err(err) = endpoint.set_alpns(alpns) {
            node.shutdown().await?;
            return Err(err);
        }

        // Wait for a single direct address update, to make sure we found at least one direct
        // address.
        let wait_for_endpoints = {
            async move {
                tokio::time::timeout(ENDPOINT_WAIT, endpoint.direct_addresses().next())
                    .await
                    .context("waiting for endpoint")?
                    .context("no endpoints")?;
                Ok(())
            }
        };

        if let Err(err) = wait_for_endpoints.await {
            node.shutdown().await.ok();
            return Err(err);
        }

        Ok(node)
    }

    pub async fn direct_addresses(&self) -> Option<Vec<DirectAddr>> {
        self.inner.endpoint.direct_addresses().next().await
    }

    /// Returns the public key of the node.
    pub fn node_id(&self) -> NodeId {
        self.inner.secret_key.public()
    }

    pub async fn connect(&self, node_addr: NodeAddr) -> Result<Connection> {
        self.inner.endpoint.connect(node_addr, BLOBS_ALPN).await
    }

    pub async fn add_blob(&self, path: PathBuf) -> impl Stream<Item = BlobAddPathResponse> {
        let (sender, receiver) = flume::bounded(32);
        let db = self.inner.db.clone();

        {
            let sender = sender.clone();
            self.inner.pool_handle.spawn_pinned(|| async move {
                if let Err(e) = add_from_path(db, path, sender.clone()).await {
                    sender.send_async(AddProgress::Abort(e.into())).await.ok();
                }
            });
        }

        receiver.into_stream().map(BlobAddPathResponse)
    }

    pub async fn blob_download(&self, hash: Hash) -> impl Stream<Item = BlobDownloadResponse> {
        let (sender, receiver) = flume::bounded(1024);
        let progress = FlumeProgressSender::new(sender);
        let downloader = self.inner.downloader.clone();
        let endpoint = self.inner.endpoint.clone();
        let nodes = self.inner.config.direct_node_addresses.clone();
        let format = BlobFormat::Raw;
        let hash_and_format = HashAndFormat { hash, format };

        self.inner.pool_handle.spawn_pinned(move || async move {
            match download_queued(
                endpoint,
                &downloader,
                hash_and_format,
                nodes,
                progress.clone(),
            )
            .await
            {
                Ok(stats) => {
                    progress.send(DownloadProgress::AllDone(stats)).await.ok();
                }
                Err(err) => {
                    progress
                        .send(DownloadProgress::Abort(err.into()))
                        .await
                        .ok();
                }
            }
        });

        receiver.into_stream().map(BlobDownloadResponse)
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
