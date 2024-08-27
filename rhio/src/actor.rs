use anyhow::{anyhow, Result};
use tokio::sync::{mpsc, oneshot};
use tracing::error;

use crate::blobs::Blobs;
use crate::nats::Nats;
use crate::p2panda::Panda;

pub enum ToNodeActor {
    Shutdown { reply: oneshot::Sender<()> },
}

pub struct NodeActor {
    inbox: mpsc::Receiver<ToNodeActor>,
    nats: Nats,
    panda: Panda,
    blobs: Blobs,
}

impl NodeActor {
    pub fn new(nats: Nats, panda: Panda, blobs: Blobs, inbox: mpsc::Receiver<ToNodeActor>) -> Self {
        Self {
            nats,
            panda,
            blobs,
            inbox,
        }
    }

    pub async fn run(mut self) -> Result<()> {
        // Take oneshot sender from outside API awaited by `shutdown` call and fire it as soon as
        // shutdown completed
        let shutdown_completed_signal = self.run_inner().await;
        if let Err(err) = self.shutdown().await {
            error!(?err, "error during shutdown");
        }

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
                Some(msg) = self.inbox.recv() => {
                    match msg {
                        ToNodeActor::Shutdown { reply } => {
                            break Ok(reply);
                        }
                        msg => {
                            if let Err(err) = self.on_actor_message(msg).await {
                                break Err(err);
                            }
                        }
                    }
                },
                else => {
                    // Error occurred outside of actor and our loop got disabled. We exit here
                    // silently, the application will probably be exited with an error message.
                    return Err(anyhow!("error outside of actor occurred"));
                }
            }
        }
    }

    async fn on_actor_message(&mut self, msg: ToNodeActor) -> Result<()> {
        match msg {
            ToNodeActor::Shutdown { .. } => {
                unreachable!("handled in run_inner");
            }
        }

        Ok(())
    }

    async fn shutdown(&self) -> Result<()> {
        self.nats.shutdown().await?;
        self.panda.shutdown().await?;
        self.blobs.shutdown().await?;
        Ok(())
    }
}
