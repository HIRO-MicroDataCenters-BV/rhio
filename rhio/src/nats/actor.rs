use anyhow::{Context, Result};
use async_nats::jetstream::Context as JetstreamContext;
use async_nats::{Client as NatsClient, ConnectOptions};
use tokio::sync::{mpsc, oneshot};
use tracing::error;

use crate::config::Config;

pub enum ToNatsActor {
    Shutdown { reply: oneshot::Sender<()> },
}

pub struct NatsActor {
    inbox: mpsc::Receiver<ToNatsActor>,
    nats_client: NatsClient,
    nats_jetstream: JetstreamContext,
}

impl NatsActor {
    pub async fn new(config: Config, inbox: mpsc::Receiver<ToNatsActor>) -> Result<Self> {
        // @TODO: Add auth options to NATS client config
        let nats_client =
            async_nats::connect_with_options(config.nats.endpoint.clone(), ConnectOptions::new())
                .await
                .context("connecting to NATS server")?;
        let nats_jetstream = async_nats::jetstream::new(nats_client.clone());

        Ok(Self {
            inbox,
            nats_client,
            nats_jetstream,
        })
    }

    pub async fn run(mut self) -> Result<()> {
        // Take oneshot sender from outside API awaited by `shutdown` call and fire it as soon as
        // shutdown completed
        let shutdown_completed_signal = self.run_inner().await;
        if let Err(err) = self.shutdown().await {
            error!(?err, "error during shutdown");
        }

        drop(self);

        match shutdown_completed_signal {
            Ok(reply_tx) => {
                reply_tx.send(()).ok();
                Ok(())
            }
            Err(err) => Err(err),
        }
    }

    async fn run_inner(&mut self) -> Result<oneshot::Sender<()>> {
        loop {
            tokio::select! {
                biased;
                msg = self.inbox.recv() => {
                    let msg = msg.context("inbox closed")?;
                    match msg {
                        ToNatsActor::Shutdown { reply } => {
                            break Ok(reply);
                        }
                        msg => {
                            if let Err(err) = self.on_actor_message(msg).await {
                                break Err(err);
                            }
                        }
                    }
                },
            }
        }
    }

    async fn on_actor_message(&mut self, msg: ToNatsActor) -> Result<()> {
        match msg {
            ToNatsActor::Shutdown { .. } => {
                unreachable!("handled in run_inner");
            }
        }

        Ok(())
    }

    async fn shutdown(&mut self) -> Result<()> {
        Ok(())
    }
}
