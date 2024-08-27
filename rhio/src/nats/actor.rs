use anyhow::{Context, Result};
use async_nats::jetstream::Context as JetstreamContext;
use async_nats::Client as NatsClient;
use tokio::sync::{mpsc, oneshot};
use tracing::error;

use crate::nats::InitialDownloadReady;

use super::consumer::consume_stream;

pub enum ToNatsActor {
    Publish {
        subject: String,
        payload: Vec<u8>,
        reply: oneshot::Sender<Result<()>>,
    },
    Subscribe {
        /// NATS stream name.
        stream_name: String,

        /// NATS subject filter.
        ///
        /// Streams can hold different subjects, with a "subject filter" we're able to only select
        /// the ones we're interested in. This forms "filtered views" on top of streams.
        filter_subject: Option<String>,

        /// Oneshot channel to propagate errors and "readyness" state.
        ///
        /// An initial downloading of all persisted data from the NATS server is required when
        /// starting to subscribe to a subject. This method returns an oneshot receiver the user
        /// can await to understand when the initialization has finished.
        reply: oneshot::Sender<Result<InitialDownloadReady>>,
    },
    Shutdown {
        reply: oneshot::Sender<()>,
    },
}

pub struct NatsActor {
    inbox: mpsc::Receiver<ToNatsActor>,
    nats_jetstream: JetstreamContext,
}

impl NatsActor {
    pub fn new(nats_client: NatsClient, inbox: mpsc::Receiver<ToNatsActor>) -> Self {
        let nats_jetstream = async_nats::jetstream::new(nats_client.clone());

        Self {
            inbox,
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
            ToNatsActor::Publish {
                subject,
                payload,
                reply,
            } => {
                let result = self.on_publish(subject, payload).await;
                reply.send(result).ok();
            }
            ToNatsActor::Subscribe {
                stream_name,
                filter_subject,
                reply,
            } => {
                let result = self.on_subscribe(stream_name, filter_subject).await;
                reply.send(result).ok();
            }
            ToNatsActor::Shutdown { .. } => {
                unreachable!("handled in run_inner");
            }
        }

        Ok(())
    }

    /// Publish a message inside an existing stream.
    ///
    /// This method fails if the stream does not exist on the NATS server
    async fn on_publish(&self, subject: String, payload: Vec<u8>) -> Result<()> {
        let server_ack = self.nats_jetstream.publish(subject, payload.into()).await?;

        // Wait until the server confirmed receiving this message, to make sure it got delivered
        // and persisted
        server_ack.await?;

        Ok(())
    }

    async fn on_subscribe(
        &self,
        stream_name: String,
        filter_subject: Option<String>,
    ) -> Result<InitialDownloadReady> {
        consume_stream(&self.nats_jetstream, stream_name, filter_subject).await
    }

    async fn shutdown(&mut self) -> Result<()> {
        Ok(())
    }
}
