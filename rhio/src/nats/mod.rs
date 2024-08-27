mod actor;

use anyhow::Result;
use p2panda_net::SharedAbortingJoinHandle;
use tokio::sync::{mpsc, oneshot};
use tracing::error;

use crate::config::Config;
use crate::nats::actor::{NatsActor, ToNatsActor};

#[derive(Debug)]
pub struct Nats {
    nats_actor_tx: mpsc::Sender<ToNatsActor>,
    #[allow(dead_code)]
    actor_handle: SharedAbortingJoinHandle<()>,
}

impl Nats {
    pub async fn new(config: Config) -> Result<Self> {
        let (nats_actor_tx, nats_actor_rx) = mpsc::channel(64);
        let nats_actor = NatsActor::new(config, nats_actor_rx).await?;

        let actor_handle = tokio::task::spawn(async move {
            if let Err(err) = nats_actor.run().await {
                error!("engine actor failed: {err:?}");
            }
        });

        Ok(Self {
            nats_actor_tx,
            actor_handle: actor_handle.into(),
        })
    }

    pub async fn shutdown(&self) -> Result<()> {
        let (reply, reply_rx) = oneshot::channel();
        self.nats_actor_tx
            .send(ToNatsActor::Shutdown { reply })
            .await?;
        reply_rx.await?;
        Ok(())
    }
}
