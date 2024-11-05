mod actor;
mod consumer;

use anyhow::{bail, Context, Result};
use async_nats::ConnectOptions;
use futures_util::future::{MapErr, Shared};
use futures_util::{FutureExt, TryFutureExt};
use rhio_core::{DeprecatedSubject, TopicId};
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::task::JoinError;
use tokio_util::task::AbortOnDropHandle;
use tracing::error;

use crate::config::Config;
use crate::nats::actor::{NatsActor, ToNatsActor};
pub use crate::nats::consumer::JetStreamEvent;
use crate::JoinErrToStr;

#[derive(Debug)]
pub struct Nats {
    nats_actor_tx: mpsc::Sender<ToNatsActor>,
    #[allow(dead_code)]
    actor_handle: Shared<MapErr<AbortOnDropHandle<()>, JoinErrToStr>>,
}

impl Nats {
    pub async fn new(config: Config) -> Result<Self> {
        let options = match config.nats.credentials {
            Some(credentials) => {
                match (
                    credentials.nkey,
                    credentials.token,
                    credentials.username,
                    credentials.password,
                ) {
                    (Some(nkey), None, None, None) => ConnectOptions::with_nkey(nkey),
                    (None, Some(token), None, None) => ConnectOptions::with_token(token),
                    (None, None, Some(username), Some(password)) => {
                        ConnectOptions::with_user_and_password(username, password)
                    }
                    _ => bail!("ambigious nats credentials configuration"),
                }
            }
            None => ConnectOptions::new(),
        };

        let nats_client = async_nats::connect_with_options(config.nats.endpoint.clone(), options)
            .await
            .context(format!(
                "connecting to NATS server {}",
                config.nats.endpoint
            ))?;

        // Start the main NATS JetStream actor to dynamically maintain "stream consumers".
        let (nats_actor_tx, nats_actor_rx) = mpsc::channel(64);
        let nats_actor = NatsActor::new(nats_client, nats_actor_rx);

        let actor_handle = tokio::task::spawn(async move {
            if let Err(err) = nats_actor.run().await {
                error!("engine actor failed: {err:?}");
            }
        });

        let actor_drop_handle = AbortOnDropHandle::new(actor_handle)
            .map_err(Box::new(|e: JoinError| e.to_string()) as JoinErrToStr)
            .shared();

        Ok(Self {
            nats_actor_tx,
            actor_handle: actor_drop_handle,
        })
    }

    /// Subscribes to a NATS Jetstream "subject" by creating a consumer hooking into a stream
    /// provided by the NATS server.
    ///
    /// All consumers in rhio are push-based, ephemeral and do not ack when a message was received.
    /// With this design we can download all past messages from the stream before we can receive
    /// future messages and rely on NATS as our persistence layer.
    ///
    /// This method creates a consumer and fails if something goes wrong. It proceeds with
    /// downloading all past data from the server; the returned channel can be used to await when
    /// that download has been finished. Finally it keeps the consumer alive in the background for
    /// handling future messages. All past and future messages are sent to the returned stream.
    pub async fn subscribe(
        &self,
        stream_name: String,
        filter_subject: Option<String>,
        topic: TopicId,
    ) -> Result<broadcast::Receiver<JetStreamEvent>> {
        let (reply, reply_rx) = oneshot::channel();
        self.nats_actor_tx
            .send(ToNatsActor::Subscribe {
                stream_name,
                filter_subject,
                topic,
                reply,
            })
            .await?;
        reply_rx.await?
    }

    pub async fn publish(
        &self,
        wait_for_ack: bool,
        subject: DeprecatedSubject,
        payload: Vec<u8>,
    ) -> Result<()> {
        let (reply, reply_rx) = oneshot::channel();
        self.nats_actor_tx
            .send(ToNatsActor::Publish {
                wait_for_ack,
                subject,
                payload,
                reply,
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
