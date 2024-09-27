mod actor;

use anyhow::Result;
use futures_util::future::{MapErr, Shared};
use futures_util::{FutureExt, TryFutureExt};
use p2panda_core::{Body, Header, Operation};
use p2panda_net::{AbortOnDropHandle, JoinErrToStr, Network};
use p2panda_store::MemoryStore;
use rhio_core::{RhioExtensions, TopicId};
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::task::JoinError;
use tracing::error;

use crate::network::actor::{PandaActor, ToPandaActor};

#[derive(Debug)]
pub struct Panda {
    panda_actor_tx: mpsc::Sender<ToPandaActor>,
    #[allow(dead_code)]
    actor_handle: Shared<MapErr<AbortOnDropHandle<()>, JoinErrToStr>>,
}

impl Panda {
    pub fn new(network: Network, store: MemoryStore<[u8; 32], RhioExtensions>) -> Self {
        let (panda_actor_tx, panda_actor_rx) = mpsc::channel(256);
        let panda_actor = PandaActor::new(network, store, panda_actor_rx);

        let actor_handle = tokio::task::spawn(async move {
            if let Err(err) = panda_actor.run().await {
                error!("p2panda actor failed: {err:?}");
            }
        });

        let actor_drop_handle = AbortOnDropHandle::new(actor_handle)
            .map_err(Box::new(|e: JoinError| e.to_string()) as JoinErrToStr)
            .shared();

        Self {
            panda_actor_tx,
            actor_handle: actor_drop_handle,
        }
    }

    /// Subscribe to a gossip topic.
    ///
    /// Returns a sender for broadcasting messages to all peers subscribed to this topic, a
    /// receiver where messages can be awaited, and future which resolves once the gossip overlay
    /// is ready.
    pub async fn subscribe(
        &self,
        topic: TopicId,
    ) -> Result<broadcast::Receiver<Operation<RhioExtensions>>> {
        let (reply, reply_rx) = oneshot::channel();
        self.panda_actor_tx
            .send(ToPandaActor::Subscribe { topic, reply })
            .await?;
        let rx = reply_rx.await?;
        Ok(rx)
    }

    /// Validates and stores a given operation in the in-memory cache.
    pub async fn ingest(
        &self,
        header: Header<RhioExtensions>,
        body: Option<Body>,
    ) -> Result<Operation<RhioExtensions>> {
        let (reply, reply_rx) = oneshot::channel();
        self.panda_actor_tx
            .send(ToPandaActor::Ingest {
                header,
                body,
                reply,
            })
            .await?;
        reply_rx.await?
    }

    /// Broadcasts an operation in the gossip overlay-network scoped by topic.
    // @TODO(adz): This should eventually be replaced with another logic when `p2panda-sync` is in
    // place, some connection mananger will pick up other peers, replicate with them and then move
    // into gossip mode.
    // See related issue: https://github.com/HIRO-MicroDataCenters-BV/rhio/issues/61
    pub async fn broadcast(
        &self,
        header: Header<RhioExtensions>,
        body: Option<Body>,
        topic: TopicId,
    ) -> Result<()> {
        let (reply, reply_rx) = oneshot::channel();
        self.panda_actor_tx
            .send(ToPandaActor::Broadcast {
                header,
                body,
                topic,
                reply,
            })
            .await?;
        reply_rx.await?
    }

    pub async fn shutdown(&self) -> Result<()> {
        let (reply, reply_rx) = oneshot::channel();
        self.panda_actor_tx
            .send(ToPandaActor::Shutdown { reply })
            .await?;
        reply_rx.await?;
        Ok(())
    }
}
