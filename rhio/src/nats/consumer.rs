use anyhow::{anyhow, Context, Result};
use async_nats::jetstream::consumer::push::Config as ConsumerConfig;
use async_nats::jetstream::consumer::{AckPolicy, PushConsumer};
use async_nats::jetstream::Context as JetstreamContext;
use tokio::sync::oneshot;
use tokio_stream::StreamExt;

use crate::nats::InitialDownloadReady;

/// Create a consumer of a NATS stream.
///
/// This method launches an async task and does the following work:
///
/// 1. Create and connect consumer to existing NATS JetStream "stream" identified by name and
///    optionally filtered with an "subject filter"
/// 2. Download all past (filtered) messages, which have been persisted in this stream
/// 3. Finally, continue streaming future messages
///
/// Streams are message stores in NATS JetStream, each stream defines how messages are stored and
/// what the limits (duration, size, interest) of the retention are. In rhio we use streams for
/// permament storage: messages are kept forever (for now). Streams consume normal NATS subjects,
/// any message published on those subjects will be captured in the defined storage system.
///
/// A consumer is a stateful view of a stream. It acts as an interface for clients to consume a
/// subset of messages stored in a stream.
///
/// The consumers used here are push-based, "un-acking" and ephemeral, meaning that no state of the
/// consumer is persisted on the NATS server and no message is marked as "read" to be able to
/// re-play them again when the process restarts.
pub async fn consume_stream(
    context: &JetstreamContext,
    stream_name: String,
    filter_subject: Option<String>,
) -> Result<InitialDownloadReady> {
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
            "create ephemeral jetstream consumer for '{}' stream",
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
                            break Err(anyhow!("could not retreive jetstream message info: {err}"));
                        }
                    };

                    println!("message: {:?}", message.message.payload);

                    // We're done with downloading all past messages
                    if message_info.stream_sequence >= num_pending {
                        break Ok(());
                    }
                }
                Some(Err(err)) => {
                    break Err(anyhow!("jetstream message error occurred: {err}"));
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
