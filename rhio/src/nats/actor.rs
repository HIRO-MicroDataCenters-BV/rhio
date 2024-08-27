use anyhow::{Context, Result};
use async_nats::jetstream::consumer::push::Config as ConsumerConfig;
use async_nats::jetstream::consumer::{AckPolicy, PushConsumer};
use async_nats::jetstream::Context as JetstreamContext;
use async_nats::{Client as NatsClient, Subject};
use tokio::sync::{mpsc, oneshot};
use tokio_stream::StreamExt;
use tracing::error;

use crate::nats::InitialDownloadReady;
use crate::topic_id::TopicId;

pub enum ToNatsActor {
    Subscribe {
        /// NATS stream name.
        stream_name: String,

        /// NATS subject.
        ///
        /// It is possible to subscribe to multiple different subjects over the same stream.
        /// Subjects form "filtered views" on top of streams.
        subject: Subject,

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
            ToNatsActor::Subscribe {
                reply,
                stream_name,
                subject,
            } => {
                let result = self.on_subscribe(stream_name, subject).await;
                reply.send(result).ok();
            }
            ToNatsActor::Shutdown { .. } => {
                unreachable!("handled in run_inner");
            }
        }

        Ok(())
    }

    async fn on_subscribe(
        &self,
        stream_name: String,
        subject: Subject,
    ) -> Result<InitialDownloadReady> {
        let (initial_download_ready_tx, initial_download_ready_rx) = oneshot::channel();

        // Generate a p2panda "topic" by hashing the subject
        let topic_id = TopicId::from_str(&subject.to_string());

        // We need to create a push based consumer as pull-based ones are required to explicitly
        // acknowledge messages.
        let mut consumer: PushConsumer = self
            .nats_jetstream
            // Streams need to already be created on the server, if not, this method will fail
            // here. Note that no checks are applied here for validating if the NATS stream
            // configuration is compatible with rhio's design
            .get_stream(stream_name)
            .await
            .context("get stream from NATS server")?
            .create_consumer(ConsumerConfig {
                // Give the consumer a name to identify it for debugging purposes, we choose the
                // p2panda topic id here which might troubleshooting
                name: Some(topic_id.to_string()),
                // Setting a delivery subject is crucial for making this consumer push-based
                deliver_subject: "rhio".to_string(),
                // We want to filter the given stream based on this subject, like this we can have
                // different "views" on the same stream
                filter_subject: subject.to_string(),
                // This is an ephemeral consumer which will not be persisted on the server and the
                // progress of the consumer will not be remembered. We do this by setting
                // "durable_name" to None.
                durable_name: None,
                // Do not ack every incoming message, as we want to receive them _again_ after rhio
                // got restarted
                ack_policy: AckPolicy::None,
                ..Default::default()
            })
            .await
            .context("create ephemeral Jetstream consumer")?;

        // Retreive info about the consumer to learn how many messages are currently persisted on
        // the server
        let consumer_info = consumer.info().await?;

        let mut messages = consumer.messages().await.context("get message stream")?;

        tokio::task::spawn(async move {
            while let Some(message) = messages.next().await {
                let message = message.unwrap();
                println!("message: {:?}", message.message.payload);
            }
        });

        Ok(initial_download_ready_rx)
    }

    async fn shutdown(&mut self) -> Result<()> {
        Ok(())
    }
}
