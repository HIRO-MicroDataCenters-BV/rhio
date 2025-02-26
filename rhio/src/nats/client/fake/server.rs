use anyhow::{anyhow, bail, Context as AnyhowContext, Result};
use async_nats::jetstream::consumer::DeliverPolicy;
use async_nats::Message;
use dashmap::DashMap;
use futures::stream::SelectAll;
use futures::Stream;
use loole::Receiver;
use loole::Sender;
use once_cell::sync::Lazy;
use rhio_config::configuration::NatsConfig;
use rhio_core::Subject;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::{error, info};

use crate::nats::client::types::NatsStreamProtocol;

type ClientId = String;
type MessageSender = Sender<NatsStreamProtocol>;
pub type MessageStream = dyn Stream<Item = NatsStreamProtocol> + Unpin + Send + Sync + 'static;

pub static TEST_FAKE_SERVER: Lazy<DashMap<NatsConfig, Arc<FakeNatsServer>>> =
    Lazy::new(|| DashMap::new());

/// Represents a subscription to a set of subjects in the fake NATS server.
///
/// A `FakeSubscription` contains a unique subscriber ID, a consumer name, and a list of subjects
/// that the subscriber is interested in. It provides functionality to check if a given subject
/// matches any of the filter subjects in the subscription.
///
/// # Fields
///
/// * `subscriber_id` - A unique identifier for the subscriber.
/// * `consumer_name` - The name of the consumer associated with the subscription.
/// * `filter_subjects` - A list of subjects that the subscriber is interested in.
///
/// # Methods
///
/// * `match_subject` - Checks if a given subject matches any of the filter subjects in the subscription.
///
#[derive(Clone, Default, Debug, PartialEq, Eq, Hash)]
pub struct FakeSubscription {
    subscriber_id: u64,
    consumer_name: String,
    filter_subjects: Vec<Subject>,
}

impl FakeSubscription {
    fn match_subject(&self, subject: &Subject) -> bool {
        self.filter_subjects
            .iter()
            .any(|filter_subject| filter_subject.is_matching(&subject))
    }
}

/// Represents a fake NATS server for testing purposes.
///
/// This server allows for the creation of subscriptions and the publishing of messages
/// to those subscriptions. It maintains a list of subscribers and their respective
/// subscriptions, and can distribute messages to the appropriate subscribers based on
/// the subject of the message. Additionally, it can simulate connection errors and
/// track failed connection attempts.
///
/// # Fields
///
/// * `subscribers` - A map of client IDs to their respective subscriptions and message senders.
/// * `storage` - A mutex-protected vector of messages that have been published to the server.
/// * `error_rx` - A receiver for error messages.
/// * `error_tx` - A sender for error messages.
/// * `subscription_ids` - An atomic counter for generating unique subscription IDs.
/// * `server_seq` - An atomic counter for generating unique message sequence numbers.
/// * `connection_error` - An atomic boolean indicating if a connection error should be simulated.
/// * `failed_connection_attempts` - An atomic counter for tracking failed connection attempts.
///
/// # Methods
///
/// * `new` - Creates a new instance of the fake NATS server.
/// * `get_by_config` - Retrieves a fake NATS server instance by configuration.
/// * `enable_connection_error` - Enables the simulation of connection errors.
/// * `disable_connection_error` - Disables the simulation of connection errors.
/// * `wait_for_connection_error` - Waits for a connection error to occur within a specified duration.
/// * `wait_for_connections` - Waits for connections to be established within a specified duration.
/// * `add_subscription` - Adds a new subscription for a client with specified filter subjects and delivery policy.
/// * `create_consumer_stream` - Creates a consumer stream from message and error receivers.
/// * `remove_subscription` - Removes a subscription for a client.
/// * `publish` - Publishes a message to the server and distributes it to the appropriate subscribers.
///
pub struct FakeNatsServer {
    subscribers: DashMap<ClientId, DashMap<FakeSubscription, MessageSender>>,
    storage: Mutex<Vec<NatsStreamProtocol>>,
    error_rx: Receiver<NatsStreamProtocol>,
    error_tx: Sender<NatsStreamProtocol>,
    subscription_ids: AtomicU64,
    server_seq: AtomicU64,
    connection_error: AtomicBool,
    failed_connection_attempts: AtomicU64,
}

impl FakeNatsServer {
    pub fn new() -> FakeNatsServer {
        let (error_tx, error_rx) = loole::bounded::<NatsStreamProtocol>(16);
        FakeNatsServer {
            subscribers: DashMap::new(),
            subscription_ids: AtomicU64::new(1),
            server_seq: AtomicU64::new(1),
            storage: Mutex::new(vec![]),
            connection_error: AtomicBool::new(false),
            failed_connection_attempts: AtomicU64::new(0),
            error_tx,
            error_rx,
        }
    }

    pub fn get_by_config(config: &NatsConfig) -> Option<Arc<FakeNatsServer>> {
        TEST_FAKE_SERVER.get(config).map(|server| server.clone())
    }

    pub fn enable_connection_error(&self) {
        self.connection_error.store(true, Ordering::SeqCst);
        if let Err(e) = self.error_tx.send(NatsStreamProtocol::ServerDisconnect) {
            error!("Error sending disconnect {:?}", e);
        }
    }

    pub fn disable_connection_error(&self) {
        self.connection_error.store(false, Ordering::SeqCst);
    }

    pub async fn wait_for_connection_error(&self, duration: std::time::Duration) -> Result<u64> {
        let failures = tokio::time::timeout(duration, async {
            while self.failed_connection_attempts.load(Ordering::SeqCst) == 0 {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            let failed_attempts = self.failed_connection_attempts.load(Ordering::SeqCst);
            self.failed_connection_attempts.store(0, Ordering::SeqCst);
            failed_attempts
        })
        .await
        .context("connection failures expected")?;
        Ok(failures)
    }

    pub async fn wait_for_connections(&self, duration: std::time::Duration) -> Result<()> {
        tokio::time::timeout(duration, async {
            while self.subscribers.len() == 0 {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .context("connections expected")?;
        Ok(())
    }
}

impl FakeNatsServer {
    pub async fn add_subscription(
        &self,
        client_id: String,
        consumer_name: String,
        filter_subjects: Vec<Subject>,
        deliver_policy: DeliverPolicy,
    ) -> Result<(FakeSubscription, SelectAll<Box<MessageStream>>)> {
        if self.connection_error.load(Ordering::SeqCst) {
            info!(%client_id, ?filter_subjects, "simulating stream creation failure");
            self.failed_connection_attempts
                .fetch_add(1, Ordering::AcqRel);
            return Err(anyhow!("simulated connection error"));
        }
        let subscriber_id = self.subscription_ids.fetch_add(1, Ordering::AcqRel);

        info!(%client_id, ?subscriber_id, ?filter_subjects, "add message subscription to FakeNatsServer");

        let (subscriber_tx, subscriber_rx) = loole::unbounded::<NatsStreamProtocol>();

        let subscription = FakeSubscription {
            subscriber_id,
            consumer_name,
            filter_subjects,
        };
        let subscription_rx =
            FakeNatsServer::create_consumer_stream(subscriber_rx, self.error_rx.clone());

        match deliver_policy {
            DeliverPolicy::All => {
                self.publish_existing_messages(&subscription, subscriber_tx.clone(), None)
                    .await
                    .context(
                        "FakeNatsServer: Publishing existing messages for delivery policy ALL",
                    )?;
            }
            DeliverPolicy::New => (),
            DeliverPolicy::ByStartSequence { start_sequence } => {
                self.publish_existing_messages(&subscription, subscriber_tx.clone(), Some(start_sequence))
                .await
                .context(
                    format!("FakeNatsServer: Publishing existing messages for delivery policy ByStartSequence {{ start_sequence: {start_sequence} }}"),
                )?;
            }
            policy => bail!("FakeNatsServer: Unimplemented deliver policy {:?}", policy),
        }

        self.subscribers
            .entry(client_id)
            .or_insert_with(DashMap::new)
            .insert(subscription.clone(), subscriber_tx);

        Ok((subscription, subscription_rx))
    }

    fn create_consumer_stream(
        messages_rx: Receiver<NatsStreamProtocol>,
        errors_rx: Receiver<NatsStreamProtocol>,
    ) -> SelectAll<Box<MessageStream>> {
        let mut messages: SelectAll<Box<MessageStream>> = SelectAll::new();
        let disconnect_messages: Box<MessageStream> = Box::new(errors_rx.clone().stream());
        let consumer_messages: Box<MessageStream> = Box::new(messages_rx.stream());
        messages.push(consumer_messages);
        messages.push(disconnect_messages);
        messages
    }

    async fn publish_existing_messages(
        &self,
        subscription: &FakeSubscription,
        subscriber_tx: Sender<NatsStreamProtocol>,
        seq_no: Option<u64>,
    ) -> Result<()> {
        let start_sequence = seq_no.unwrap_or(0);
        let storage = self.storage.lock().await;
        for protocol_message in storage.iter() {
            let subject = match protocol_message {
                NatsStreamProtocol::Msg { msg, seq } => {
                    if seq.map(|s| s < start_sequence).unwrap_or(false) {
                        continue;
                    }
                    msg.subject.as_str()
                }
                _ => continue,
            };
            let subject = rhio_core::Subject::from_str(subject)?;
            if subscription.match_subject(&subject) {
                subscriber_tx
                    .send(protocol_message.clone())
                    .context("FakeNatsClient: Publishing messages")?;
            }
        }
        Ok(())
    }

    pub fn remove_subscription(&self, client_id: &String, subscription: &FakeSubscription) {
        info!(%client_id, client_id, ?subscription, "drop subscription from FakeNatsServer");
        if let Some(subscribers) = self.subscribers.get(client_id) {
            subscribers.remove(&subscription);
        }
    }

    pub async fn publish(&self, subject: Subject, msg: Message) -> Result<()> {
        if self.connection_error.load(Ordering::SeqCst) {
            info!(%subject, ?msg, "simulating stream creation failure");
            self.failed_connection_attempts
                .fetch_add(1, Ordering::AcqRel);
            return Err(anyhow!("simulated connection error"));
        }
        let seq = Some(self.server_seq.fetch_add(1, Ordering::AcqRel));
        let message = NatsStreamProtocol::Msg { msg, seq };
        self.persist_message(&message).await;
        self.distribute_to_subscribers(subject, message)
    }

    async fn persist_message(&self, message: &NatsStreamProtocol) {
        let mut storage = self.storage.lock().await;
        storage.push(message.clone());
    }

    fn distribute_to_subscribers(
        &self,
        subject: Subject,
        message: NatsStreamProtocol,
    ) -> Result<()> {
        for subscription_entry in self.subscribers.iter() {
            let subscriptions = subscription_entry.value();

            for subscription in subscriptions.iter() {
                if subscription.key().match_subject(&subject) {
                    subscription.value().send(message.clone()).context(format!(
                        "FakeNatsClient: Send message to subscriber {:?}",
                        subscription.key()
                    ))?;
                }
            }
        }
        Ok(())
    }
}
