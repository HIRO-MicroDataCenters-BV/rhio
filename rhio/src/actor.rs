use anyhow::{anyhow, Result};
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
                Some(Ok(event)) = self.nats_consumer_rx.next() => {
                    info!("{event:?}");
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

    async fn on_subscribe(
        &mut self,
        stream_name: String,
        filter_subject: Option<String>,
    ) -> Result<()> {
        let rx = self.nats.subscribe(stream_name, filter_subject).await?;
        self.nats_consumer_rx.push(BroadcastStream::new(rx));

        // tokio::spawn(async move {
        //     loop {
        //         tokio::select! {
        //             Ok(event) = nats_consumer_rx.recv() => {
        //                 match event {
        //                     ConsumerEvent::InitializationCompleted => {
        //                         info!("initialization succeeded");
        //                     },
        //                     ConsumerEvent::InitializationFailed => {
        //                         error!("initialization failed");
        //                     },
        //                     ConsumerEvent::StreamFailed => {
        //                         error!("stream failed");
        //                     },
        //                     ConsumerEvent::Message { payload, .. } => {
        //                         info!("message received {:?}", payload);
        //                     },
        //                 }
        //             },
        //             Ok(_) = tokio::signal::ctrl_c() => {
        //                 break;
        //             },
        //         }
        //     }
        // });

        Ok(())
    }

    async fn shutdown(&self) -> Result<()> {
        self.nats.shutdown().await?;
        self.panda.shutdown().await?;
        self.blobs.shutdown().await?;
        Ok(())
    }
}
