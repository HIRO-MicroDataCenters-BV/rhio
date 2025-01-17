use crate::config::NatsCredentials;
use crate::{config, StreamName};
use anyhow::{bail, Context as AnyhowContext, Result};
use async_nats::jetstream::consumer::push::MessagesError;
use async_nats::jetstream::consumer::push::{Config as ConsumerConfig, Messages};
use async_nats::jetstream::consumer::{AckPolicy, DeliverPolicy};
use async_nats::jetstream::consumer::{Info, PushConsumer};
use async_nats::jetstream::context::Publish;
use async_nats::jetstream::Context as JetstreamContext;
use async_nats::{Client, Message};
use async_nats::{ConnectOptions, HeaderMap};
use async_trait::async_trait;
use bytes::Bytes;
use pin_project::pin_project;
use rhio_core::{subjects_to_str, Subject};
use tracing::{span, trace, Level};

use super::types::{NatsClient, NatsMessageStream};

/// Implementation of the `NatsClient` trait for interacting with NATS JetStream.
///
/// This struct provides methods to publish messages to NATS subjects and create consumers for
/// NATS streams. The consumers are push-based, ephemeral, and do not acknowledge messages, allowing
/// them to be replayed upon process restart.
///
/// # Methods
///
/// - `new`: Creates a new instance of `NatsClientImpl` by connecting to the NATS server.
/// - `publish`: Publishes a message to a NATS subject with optional headers and waits for an acknowledgment if specified.
/// - `create_consumer_stream`: Creates a push-based, ephemeral consumer for a NATS stream with specified filter subjects and delivery policy.
pub struct NatsClientImpl {
    jetstream: JetstreamContext,
    client: Client,
}

impl NatsClientImpl {
    #[allow(dead_code)]
    pub async fn new(nats: config::NatsConfig) -> Result<Self> {
        let nats_options = connect_options(nats.credentials.clone())?;
        let client = async_nats::connect_with_options(nats.endpoint.clone(), nats_options)
            .await
            .context(format!("connecting to NATS server {}", nats.endpoint))?;

        let jetstream = async_nats::jetstream::new(client.clone());
        Ok(NatsClientImpl { jetstream, client })
    }
}

#[async_trait]
impl NatsClient<NatsMessages> for NatsClientImpl {
    async fn publish(
        &self,
        wait_for_ack: bool,
        subject: String,
        payload: Bytes,
        headers: Option<HeaderMap>,
    ) -> Result<()> {
        let mut publish = Publish::build().payload(payload);
        if let Some(headers) = headers {
            publish = publish.headers(headers);
        }

        let server_ack = self
            .jetstream
            .send_publish(subject, publish)
            .await
            .context("publish message to nats server")?;

        // Wait until the server confirmed receiving this message, to make sure it got delivered
        // and persisted.
        if wait_for_ack {
            server_ack
                .await
                .context("acknowledgement of the published message")?;
        }

        Ok(())
    }
    /// Create a consumer for a NATS stream and returns its wrapper for testability purposes.
    ///
    /// The consumers used here are push-based, "un-acking" and ephemeral, meaning that no state of
    /// the consumer is persisted on the NATS server and no message is marked as "read" to be able
    /// to re-play them again when the process restarts.
    ///
    /// Since NATS streams are also used for persistance with their own wide range of limit
    /// configurations, rhio does not create any streams automatically but merely consumes them.
    /// This allows rhio operators to have full flexibility over the nature of the stream. This is
    /// why for every published subject a "stream name" needs to be mentioned.
    async fn create_consumer_stream(
        &self,
        stream_name: StreamName,
        filter_subjects: Vec<Subject>,
        deliver_policy: DeliverPolicy,
    ) -> Result<(NatsMessages, Info)> {
        let mut consumer: PushConsumer = self
            .jetstream
            // Streams need to already be created on the server, if not, this method will fail
            // here. Note that no checks are applied here for validating if the NATS stream
            // configuration is compatible with rhio's design.
            .get_stream(&stream_name)
            .await
            .context(format!(
                "create or get '{}' stream from nats server",
                stream_name,
            ))?
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
                deliver_subject: {
                    // @NOTE(adz): Another thing I couldn't find documented was that if this
                    // delivery subject is the same across consumers, they'll all consume messages
                    // at the same time, which we avoid here by giving each consumer an unique,
                    // random identifier:
                    // @NOTE(ktatarnikov): The async_nats library example of push consumer
                    // (https://github.com/nats-io/nats.rs/blob/main/async-nats/examples/jetstream_push.rs)
                    // uses `client.new_inbox()` method to generate deliver_subject.
                    // The method provides stronger guarantees (globally unique) then we used previously with `random`.
                    // https://docs.rs/async-nats/0.38.0/async_nats/client/struct.Client.html#method.new_inbox
                    //
                    self.client.new_inbox()
                },
                // For rhio two different delivery policies are configured:
                //
                // 1. Live-Mode: We're only interested in _upcoming_ messages as this consumer will
                //    only be used to forward NATS messages into the gossip overlay. This happens
                //    when a rhio node decided to "publish" a NATS subject, the created consumer
                //    lives as long as the process.
                // 2. Sync-Session: Here we want to load and exchange _past_ messages, usually
                //    loading all messages from after a given timestamp. This happens when a remote
                //    rhio node requests data from a NATS subject from us, the created consumer
                //    lives as long as the sync session with this remote peer.
                // TODO (konstantin) the sync session case needs to be tested
                deliver_policy,
                // We filter the given stream based on this subject filter, like this we can have
                // different "views" on the same stream.
                filter_subjects: filter_subjects
                    .iter()
                    .map(|subject| subject.to_string())
                    .collect(),
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
                stream_name,
            ))?;

        // Retrieve info about the consumer to learn how many messages are currently persisted on
        // the server (number of "pending messages"). These are the messages we need to download
        // first before we can continue.
        let consumer_info = consumer.info().await?.clone();
        let consumer_name = consumer_info.name.clone();
        let num_pending = consumer_info.num_pending;

        let span = span!(Level::TRACE, "consumer", id = %consumer_name);
        let deliver_policy_str = match deliver_policy {
            DeliverPolicy::All => "all",
            DeliverPolicy::New => "new",
            _ => unimplemented!(),
        };
        let filter_subjects_str = subjects_to_str(filter_subjects);
        trace!(
            parent: &span,
            stream = %stream_name,
            subject = %filter_subjects_str,
            deliver_policy = deliver_policy_str,
            num_pending = num_pending,
            "create consumer for NATS"
        );

        let messages = consumer.messages().await.context("get message stream")?;

        Ok((NatsMessages { messages }, consumer_info))
    }
}

#[pin_project]
pub struct NatsMessages {
    #[pin]
    messages: Messages,
}

impl NatsMessageStream for NatsMessages {}

impl futures::Stream for NatsMessages {
    type Item = Result<Message, MessagesError>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.project();
        this.messages.poll_next(cx).map_ok(|message_with_context| {
            // We decouple async message from the inner static message contents
            // for testability purposes.
            // The `context` from async message is not used later anymore.
            let (msg, _) = message_with_context.split();
            msg
        })
    }
}

fn connect_options(config: Option<NatsCredentials>) -> Result<ConnectOptions> {
    let Some(credentials) = config else {
        return Ok(ConnectOptions::default());
    };

    let options = match (
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
    };

    Ok(options)
}
