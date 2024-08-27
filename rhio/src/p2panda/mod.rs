mod actor;

use anyhow::Result;
use p2panda_net::SharedAbortingJoinHandle;
use tokio::sync::{mpsc, oneshot};
use tracing::error;

use crate::p2panda::actor::{PandaActor, ToPandaActor};

#[derive(Debug)]
pub struct Panda {
    panda_actor_tx: mpsc::Sender<ToPandaActor>,
    #[allow(dead_code)]
    actor_handle: SharedAbortingJoinHandle<()>,
}

impl Panda {
    pub fn new() -> Self {
        let (panda_actor_tx, panda_actor_rx) = mpsc::channel(256);
        let panda_actor = PandaActor::new(panda_actor_rx);

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

    pub async fn shutdown(&self) -> Result<()> {
        let (reply, reply_rx) = oneshot::channel();
        self.panda_actor_tx
            .send(ToPandaActor::Shutdown { reply })
            .await?;
        reply_rx.await?;
        Ok(())
    }
}
