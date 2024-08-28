use anyhow::{anyhow, bail, Result};
use futures_util::stream::SelectAll;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;
use tracing::{error, info};

use crate::blobs::Blobs;
use crate::nats::{ConsumerEvent, Nats};
use crate::p2panda::Panda;

pub enum ToNodeActor {
    Subscribe {
        stream_name: String,
        filter_subject: Option<String>,
        reply: oneshot::Sender<Result<()>>,
    },
    Shutdown {
        reply: oneshot::Sender<()>,
    },
}

pub struct NodeActor {
    inbox: mpsc::Receiver<ToNodeActor>,
    nats_consumer_rx: SelectAll<BroadcastStream<ConsumerEvent>>,
    nats: Nats,
    panda: Panda,
    blobs: Blobs,
}

impl NodeActor {
    pub fn new(nats: Nats, panda: Panda, blobs: Blobs, inbox: mpsc::Receiver<ToNodeActor>) -> Self {
        Self {
            nats,
            nats_consumer_rx: SelectAll::new(),
            panda,
            blobs,
            inbox,
        }
    }

    pub async fn run(mut self) -> Result<()> {
        // Take oneshot sender from external API awaited by `shutdown` call and fire it as soon as
        // shutdown completed to signal
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
                Some(Ok(message)) = self.nats_consumer_rx.next() => {
                    if let Err(err) = self.on_nats_message(message).await {
                        break Err(err);
                    }
                },
                else => {
                    // Error occurred outside of actor and our select! loop got disabled. We exit
                    // here with an error which will probably be overriden by the external error
                    // which caused the problem in first hand.
                    break Err(anyhow!("all select! branches are disabled"));
                }
            }
        }
    }

    async fn on_actor_message(&mut self, msg: ToNodeActor) -> Result<()> {
        match msg {
            ToNodeActor::Subscribe {
                stream_name,
                filter_subject,
                reply,
            } => {
                let result = self.on_subscribe(stream_name, filter_subject).await;
                reply.send(result).ok();
            }
            ToNodeActor::Shutdown { .. } => {
                unreachable!("handled in run_inner");
            }
        }

        Ok(())
    }

    async fn on_nats_message(&mut self, message: ConsumerEvent) -> Result<()> {
        match message {
            ConsumerEvent::InitializationCompleted => {
                // @TODO
                // - Subscribe to topic in p2panda network (starting sync tasks and gossiping)
            }
            ConsumerEvent::InitializationFailed => {
                // @TODO: Better error message
                bail!("initialisation of stream failed");
            }
            ConsumerEvent::StreamFailed => {
                // @TODO: Better error message
                bail!("stream failed");
            }
            ConsumerEvent::Message {
                payload,
                subject: _,
            } => {
                // @TODO
                // - Decode and validate operation in message payload
                // - Persist every message coming in here in p2panda in-memory cache
                // - If stream is already initialized, send message to p2panda network
                info!("{payload:?}");
            }
        }

        Ok(())
    }

    async fn on_subscribe(
        &mut self,
        stream_name: String,
        filter_subject: Option<String>,
    ) -> Result<()> {
        let rx = self.nats.subscribe(stream_name, filter_subject).await?;
        // Wrap broadcast receiver stream into tokio helper, to make it implement the `Stream`
        // trait which is required by `SelectAll`
        self.nats_consumer_rx.push(BroadcastStream::new(rx));
        Ok(())
    }

    async fn shutdown(&self) -> Result<()> {
        self.nats.shutdown().await?;
        self.panda.shutdown().await?;
        self.blobs.shutdown().await?;
        Ok(())
    }
}
