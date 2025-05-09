mod actor;
pub mod membership;
pub mod sync;

use anyhow::Result;
use futures_util::future::{MapErr, Shared};
use futures_util::{FutureExt, TryFutureExt};
use p2panda_net::Network;
use p2panda_net::network::FromNetwork;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinError;
use tokio_util::task::AbortOnDropHandle;
use tracing::error;

use crate::JoinErrToStr;
use crate::network::actor::{PandaActor, ToPandaActor};
use crate::topic::Query;

#[derive(Debug)]
pub struct Panda {
    panda_actor_tx: mpsc::Sender<ToPandaActor>,
    #[allow(dead_code)]
    actor_handle: Shared<MapErr<AbortOnDropHandle<()>, JoinErrToStr>>,
}

impl Panda {
    pub fn new(network: Network<Query>) -> Self {
        let (panda_actor_tx, panda_actor_rx) = mpsc::channel(512);
        let panda_actor = PandaActor::new(network, panda_actor_rx);

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

    /// Subscribe to a data stream in the network.
    pub async fn subscribe(&self, query: Query) -> Result<Option<mpsc::Receiver<FromNetwork>>> {
        let (reply, reply_rx) = oneshot::channel();
        self.panda_actor_tx
            .send(ToPandaActor::Subscribe { query, reply })
            .await?;
        let rx = reply_rx.await?;
        Ok(rx)
    }

    /// Broadcasts a message in the gossip overlay-network with this topic id.
    pub async fn broadcast(&self, payload: Vec<u8>, topic_id: [u8; 32]) -> Result<()> {
        let (reply, reply_rx) = oneshot::channel();
        self.panda_actor_tx
            .send(ToPandaActor::Broadcast {
                payload,
                topic_id,
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
