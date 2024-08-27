use anyhow::{anyhow, Context, Result};
use async_nats::jetstream::consumer::push::Config as ConsumerConfig;
use async_nats::jetstream::consumer::{AckPolicy, DeliverPolicy, PushConsumer};
use async_nats::jetstream::Context as JetstreamContext;
use async_nats::Client as NatsClient;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::StreamExt;
use tracing::error;

use crate::nats::InitialDownloadReady;
use crate::topic_id::TopicId;

pub enum ToNatsActor {
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
            ToNatsActor::Subscribe {
                reply,
                stream_name,
                filter_subject,
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

    async fn on_subscribe(
        &self,
        stream_name: String,
        filter_subject: Option<String>,
    ) -> Result<InitialDownloadReady> {
        let (initial_download_ready_tx, initial_download_ready_rx) = oneshot::channel();

        // Generate a p2panda "topic" by hashing the Jetstream "name" and "filter subject"
        let topic_id = TopicId::from_str(&format!(
            "{}{}",
            stream_name,
            filter_subject.clone().unwrap_or_default()
        ));

        // A consumer is a stateful view of a stream. It acts as an interface for clients to
        // consume a subset of messages stored in a stream.
        let mut consumer: PushConsumer = self
            .nats_jetstream
            // Streams need to already be created on the server, if not, this method will fail
            // here. Note that no checks are applied here for validating if the NATS stream
            // configuration is compatible with rhio's design
            .get_stream(&stream_name)
            .await
            .context(format!("get '{}' stream from NATS server", stream_name))?
            .create_consumer(ConsumerConfig {
                // Give consumer a description which might aid troubleshooting.
                description: Some(format!("topic id: {topic_id}")),
                // Setting a delivery subject is crucial for making this consumer push-based. We
                // need to create a push based consumer as pull-based ones are required to
                // explicitly acknowledge messages.
                //
                // @NOTE(adz): Unclear to me what this really does other than it is required to be
                // set for push-consumers? The documentation says: "The subject to deliver messages
                // to. Setting this field decides whether the consumer is push or pull-based. With
                // a deliver subject, the server will push messages to clients subscribed to this
                // subject." https://docs.nats.io/nats-concepts/jetstream/consumers#push-specific
                //
                // .. it seems to not matter what the value inside this field is, we will still
                // receive all messages from that stream, optionally filtered by "filter_subject"?
                deliver_subject: "rhio".to_string(),
                // We can optionally filter the given stream based on this subject filter, like
                // this we can have different "views" on the same stream.
                filter_subject: filter_subject.unwrap_or_default(),
                // This is an ephemeral consumer which will not be persisted on the server / the
                // progress of the consumer will not be remembered. We do this by _not_ setting
                // "durable_name".
                durable_name: None,
                // Do _not_ acknowledge every incoming message, as we want to receive them _again_
                // after rhio got restarted. The to-be-consumed stream needs to accommodate for
                // this setting and accept an unlimited amount of un-acked message deliveries.
                ack_policy: AckPolicy::None,
                ..Default::default()
            })
            .await
            .context(format!(
                "create ephemeral Jetstream consumer for '{}' stream",
                stream_name
            ))?;

        // Retreive info about the consumer to learn how many messages are currently persisted on
        // the server (number of "pending messages") and we need to download first
        let num_pending = {
            let consumer_info = consumer.info().await?;
            consumer_info.num_pending
        };

        // Download all known messages from NATS server first
        let mut messages = consumer.messages().await.context("get message stream")?;
        tokio::task::spawn(async move {
            let result: Result<()> = loop {
                let message = messages.next().await;

                match message {
                    Some(Ok(message)) => {
                        let message_info = match message.info() {
                            Ok(info) => info,
                            Err(err) => {
                                break Err(anyhow!(
                                    "could not retreive Jetstream message info: {err}"
                                ));
                            }
                        };

                        println!("message: {:?}", message.message.payload);

                        // We're done with downloading all past messages
                        if message_info.stream_sequence >= num_pending {
                            break Ok(());
                        }
                    }
                    Some(Err(err)) => {
                        break Err(anyhow!("Jetstream message error occurred: {err}"));
                    }
                    None => (),
                }
            };

            initial_download_ready_tx.send(result).expect(&format!(
                "send initial download ready signal for '{}' stream",
                stream_name,
            ));
        });

        Ok(initial_download_ready_rx)
    }

    async fn shutdown(&mut self) -> Result<()> {
        Ok(())
    }
}

async fn handle_nats_stream() {}
