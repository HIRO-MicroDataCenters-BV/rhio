use anyhow::{Context, Result};
use async_nats::jetstream::Context as JetstreamContext;
use async_nats::{Client as NatsClient, Subject};
use tokio::sync::{mpsc, oneshot};
use tracing::error;

use crate::nats::InitialDownloadReady;

pub enum ToNatsActor {
    Subscribe {
        reply: oneshot::Sender<Result<InitialDownloadReady>>,
        subject: Subject,
    },
    Shutdown {
        reply: oneshot::Sender<()>,
    },
}

pub struct NatsActor {
    inbox: mpsc::Receiver<ToNatsActor>,
    nats_client: NatsClient,
    nats_jetstream: JetstreamContext,
}

impl NatsActor {
    pub fn new(nats_client: NatsClient, inbox: mpsc::Receiver<ToNatsActor>) -> Self {
        let nats_jetstream = async_nats::jetstream::new(nats_client.clone());

        Self {
            inbox,
            nats_client,
            nats_jetstream,
        }
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
            ToNatsActor::Subscribe { reply, subject } => {
                let result = self.on_subscribe(subject).await;
                reply.send(result).ok();
            }
            ToNatsActor::Shutdown { .. } => {
                unreachable!("handled in run_inner");
            }
        }

        Ok(())
    }

    async fn on_subscribe(&self, subject: Subject) -> Result<InitialDownloadReady> {
        let (initial_download_ready_tx, initial_download_ready_rx) = oneshot::channel();
        Ok(initial_download_ready_rx)
    }

    async fn shutdown(&mut self) -> Result<()> {
        Ok(())
    }
}
