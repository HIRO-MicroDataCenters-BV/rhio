mod actor;
mod consumer;

use std::path::PathBuf;
use std::str::FromStr;

use anyhow::{Context, Result};
use async_nats::{Client, ConnectOptions};
use p2panda_net::SharedAbortingJoinHandle;
use rhio_core::{Subject, TopicId};
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio_stream::StreamExt;
use tracing::{error, warn};

use crate::config::Config;
use crate::nats::actor::{NatsActor, ToNatsActor};
pub use crate::nats::consumer::JetStreamEvent;
use crate::node::NodeControl;

#[derive(Debug)]
pub struct Nats {
    nats_actor_tx: mpsc::Sender<ToNatsActor>,
    #[allow(dead_code)]
    actor_handle: SharedAbortingJoinHandle<()>,
}

impl Nats {
    pub async fn new(config: Config, node_control_tx: mpsc::Sender<NodeControl>) -> Result<Self> {
        // @TODO: Add auth options to NATS client config
        let nats_client =
            async_nats::connect_with_options(config.nats.endpoint.clone(), ConnectOptions::new())
                .await
                .context(format!(
                    "connecting to NATS server {}",
                    config.nats.endpoint
                ))?;

        // Start a "standard" NATS Core subscription (at-most-once delivery) to receive "live"
        // control commands for `rhio`
        spawn_control_handler(nats_client.clone(), node_control_tx);

        // Start the main NATS JetStream actor to dynamically maintain "stream consumers"
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

    pub async fn publish(&self, subject: Subject, payload: Vec<u8>) -> Result<()> {
        let (reply, reply_rx) = oneshot::channel();
        self.nats_actor_tx
            .send(ToNatsActor::Publish {
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

// @TODO(adz): This might not be the ideal flow or place for it and serves as a "workaround". We
// can keep it here until we've finished the full MinIO store backend implementation, even though
// _some_ import control needs to be given in any case?
fn spawn_control_handler(nats_client: Client, node_control_tx: mpsc::Sender<NodeControl>) {
    tokio::spawn(async move {
        let Ok(mut subscription) = nats_client.subscribe("rhio.*").await else {
            error!("failed subscribing to minio control messages");
            return;
        };

        while let Some(message) = subscription.next().await {
            if message.subject.as_str() != "rhio.import" {
                continue;
            }

            let Ok(file_path_str) = std::str::from_utf8(&message.payload) else {
                warn!("failed parsing minio control message");
                continue;
            };

            let Ok(file_path) = PathBuf::from_str(file_path_str) else {
                warn!("failed parsing minio control message");
                continue;
            };

            if let Err(err) = node_control_tx
                .send(NodeControl::ImportBlob {
                    file_path,
                    reply_subject: message.reply,
                })
                .await
            {
                error!("failed handling minio control message: {err}");
                break;
            }
        }
    });
}
