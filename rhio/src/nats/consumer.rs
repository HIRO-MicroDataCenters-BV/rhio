use anyhow::{Context, Result};
use async_nats::jetstream::consumer::push::{Config as ConsumerConfig, Messages};
use async_nats::jetstream::consumer::{AckPolicy, PushConsumer};
use async_nats::jetstream::Context as JetstreamContext;
use p2panda_net::SharedAbortingJoinHandle;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::StreamExt;
use tracing::error;

pub type StreamName = String;

pub type FilterSubject = Option<String>;

#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct ConsumerId(StreamName, FilterSubject);

impl ConsumerId {
    pub fn new(stream_name: String, filter_subject: Option<String>) -> Self {
        Self(stream_name, filter_subject)
    }
}

pub type InitialDownloadReady = oneshot::Receiver<Result<()>>;

pub enum ToConsumerActor {}

/// Manages a NATS JetStream consumer.
///
/// A consumer is a stateful view of a stream. It acts as an interface for clients to consume a
/// subset of messages stored in a stream.
///
/// Streams are message stores in NATS JetStream, each stream defines how messages are stored and
/// what the limits (duration, size, interest) of the retention are. In rhio we use streams for
/// permament storage: messages are kept forever (for now). Streams consume normal NATS subjects,
/// any message published on those subjects will be captured in the defined storage system.
pub struct ConsumerActor {
    inbox: mpsc::Receiver<ToConsumerActor>,
    stream_name: StreamName,
    messages: Messages,
    filter_subject: FilterSubject,
    initial_stream_height: u64,
    initial_download_ready_tx: oneshot::Sender<Result<()>>,
}

impl ConsumerActor {
    pub fn new(
        inbox: mpsc::Receiver<ToConsumerActor>,
        stream_name: StreamName,
        filter_subject: FilterSubject,
        messages: Messages,
        initial_stream_height: u64,
        initial_download_ready_tx: oneshot::Sender<Result<()>>,
    ) -> Self {
        Self {
            inbox,
            stream_name,
            filter_subject,
            messages,
            initial_stream_height,
            initial_download_ready_tx,
        }
    }

    pub async fn run(mut self) -> Result<()> {
        self.run_inner().await;
        Ok(())
    }

    async fn run_inner(&mut self) {
        loop {
            tokio::select! {
                biased;
                Some(next) = self.messages.next() => {
                    // @TODO
                },
            }
        }
    }
}

// let result: Result<()> = loop {
//     let message = messages.next().await;
//
//     match message {
//         Some(Ok(message)) => {
//             let message_info = match message.info() {
//                 Ok(info) => info,
//                 Err(err) => {
//                     break Err(anyhow!("could not retreive jetstream message info: {err}"));
//                 }
//             };
//
//             println!("message: {:?}", message.message.payload);
//
//             // We're done with downloading all past messages
//             if message_info.stream_sequence >= stream_height {
//                 break Ok(());
//             }
//         }
//         Some(Err(err)) => {
//             break Err(anyhow!("jetstream message error occurred: {err}"));
//         }
//         None => (),
//     }
// };
//
// initial_download_ready_tx.send(result).expect(&format!(
//     "send initial download ready signal for '{}' stream",
//     stream_name,
// ));

pub struct Consumer {
    consumer_actor_tx: mpsc::Sender<ToConsumerActor>,
    #[allow(dead_code)]
    actor_handle: SharedAbortingJoinHandle<()>,
}

impl Consumer {
    /// Create a consumer of a NATS stream.
    ///
    /// This method launches an async task and does the following work:
    ///
    /// 1. Create and connect consumer to existing NATS JetStream "stream" identified by name and
    ///    optionally filtered with an "subject filter"
    /// 2. Download all past (filtered) messages, which have been persisted in this stream
    /// 3. Finally, continue streaming future messages
    ///
    /// The consumers used here are push-based, "un-acking" and ephemeral, meaning that no state of
    /// the consumer is persisted on the NATS server and no message is marked as "read" to be able
    /// to re-play them again when the process restarts.
    pub async fn new(
        context: &JetstreamContext,
        stream_name: StreamName,
        filter_subject: FilterSubject,
    ) -> Result<(Self, InitialDownloadReady)> {
        let (initial_download_ready_tx, initial_download_ready_rx) = oneshot::channel();

        let mut consumer: PushConsumer = context
            // Streams need to already be created on the server, if not, this method will fail
            // here. Note that no checks are applied here for validating if the NATS stream
            // configuration is compatible with rhio's design
            .get_stream(&stream_name)
            .await
            .context(format!("get '{}' stream from NATS server", stream_name))?
            .create_consumer(ConsumerConfig {
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
                filter_subject: filter_subject.clone().unwrap_or_default(),
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
                "create ephemeral jetstream consumer for '{}' stream",
                stream_name
            ))?;

        // Retreive info about the consumer to learn how many messages are currently persisted on
        // the server (number of "pending messages") and we need to download first
        let initial_stream_height = {
            let consumer_info = consumer.info().await?;
            consumer_info.num_pending + 1 // sequence numbers start with 1 in NATS JetStream
        };

        let messages = consumer.messages().await.context("get message stream")?;

        let (consumer_actor_tx, consumer_actor_rx) = mpsc::channel(64);

        let consumer_actor = ConsumerActor::new(
            consumer_actor_rx,
            stream_name,
            filter_subject,
            messages,
            initial_stream_height,
            initial_download_ready_tx,
        );

        let actor_handle = tokio::task::spawn(async move {
            if let Err(err) = consumer_actor.run().await {
                error!("consumer actor failed: {err:?}");
            }
        });

        let consumer = Self {
            consumer_actor_tx,
            actor_handle: actor_handle.into(),
        };

        Ok((consumer, initial_download_ready_rx))
    }
}
