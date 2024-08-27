mod actor;
mod consumer;

use anyhow::{Context, Result};
use async_nats::ConnectOptions;
use p2panda_net::SharedAbortingJoinHandle;
use tokio::sync::{mpsc, oneshot};
use tracing::error;

use crate::config::Config;
use crate::nats::actor::{NatsActor, ToNatsActor};
pub use crate::nats::consumer::InitialDownloadReady;

#[derive(Debug)]
pub struct Nats {
    nats_actor_tx: mpsc::Sender<ToNatsActor>,
    #[allow(dead_code)]
    actor_handle: SharedAbortingJoinHandle<()>,
}

impl Nats {
    pub async fn new(config: Config) -> Result<Self> {
        // @TODO: Add auth options to NATS client config
        let nats_client =
            async_nats::connect_with_options(config.nats.endpoint.clone(), ConnectOptions::new())
                .await
                .context(format!(
                    "connecting to NATS server {}",
                    config.nats.endpoint
                ))?;

        let (nats_actor_tx, nats_actor_rx) = mpsc::channel(64);
        let nats_actor = NatsActor::new(nats_client, nats_actor_rx);

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

    /// Subscribes to a NATS Jetstream "subject" by creating a consumer hooking into a stream
    /// provided by the NATS server.
    ///
    /// All consumers in rhio are push-based, ephemeral and do not ack when a message was received.
    /// With this design we can download all past messages from the stream before we can receive
    /// future messages and rely on NATS as our persistence layer.
    ///
    /// This method creates a consumer and fails if something goes wrong with that. Then it
    /// proceeds with downloading all past data from the server, the returned oneshot receiver can
    /// be used to await when that download has been finished. Finally it keeps the consumer alive
    /// in the background for handling future messages.
    pub async fn subscribe(
        &self,
        stream_name: String,
        filter_subject: Option<String>,
    ) -> Result<InitialDownloadReady> {
        let (reply, reply_rx) = oneshot::channel();
        self.nats_actor_tx
            .send(ToNatsActor::Subscribe {
                reply,
                stream_name,
                filter_subject,
            })
            .await?;
        reply_rx.await?
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
