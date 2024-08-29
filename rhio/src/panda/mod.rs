mod actor;
mod extensions;
mod messages;
mod operations;
mod topic_id;

use std::future::Future;
use std::pin::Pin;

use anyhow::Result;
use p2panda_net::{Network, SharedAbortingJoinHandle};
use tokio::sync::{broadcast, mpsc, oneshot};
use tracing::error;

use crate::panda::actor::{PandaActor, ToPandaActor};
pub use crate::panda::messages::{Message, MessageMeta};
pub use crate::panda::topic_id::TopicId;

#[derive(Debug)]
pub struct Panda {
    panda_actor_tx: mpsc::Sender<ToPandaActor>,
    #[allow(dead_code)]
    actor_handle: SharedAbortingJoinHandle<()>,
}

impl Panda {
    pub fn new(network: Network) -> Self {
        let (panda_actor_tx, panda_actor_rx) = mpsc::channel(256);
        let panda_actor = PandaActor::new(network, panda_actor_rx);

        let actor_handle = tokio::task::spawn(async move {
            if let Err(err) = panda_actor.run().await {
                error!("p2panda actor failed: {err:?}");
            }
        });

        Self {
            panda_actor_tx,
            actor_handle: actor_handle.into(),
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
    ) -> Result<(
        broadcast::Receiver<(Message, MessageMeta)>,
        Pin<Box<dyn Future<Output = ()> + Send>>,
    )> {
        let (reply, reply_rx) = oneshot::channel();
        self.panda_actor_tx
            .send(ToPandaActor::Subscribe { topic, reply })
            .await?;
        let result = reply_rx.await?;
        result.map(|(rx, ready)| (rx, ready))
    }

    pub async fn store(&self) -> Result<()> {
        // @TODO
        // let (reply, reply_rx) = oneshot::channel();
        // self.panda_actor_tx
        //     .send(ToPandaActor::Store { reply })
        //     .await?;
        // reply_rx.await?;
        Ok(())
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
