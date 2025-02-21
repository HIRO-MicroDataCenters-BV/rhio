use anyhow::{anyhow, bail, Context as AnyhowContext, Result};
use async_nats::jetstream::consumer::push::{MessagesError, MessagesErrorKind};
use async_nats::jetstream::consumer::DeliverPolicy;
use async_nats::Message;
use dashmap::DashMap;
use loole::Receiver;
use loole::Sender;
use once_cell::sync::Lazy;
use rhio_config::configuration::NatsConfig;
use rhio_core::Subject;
use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::task::yield_now;
use tracing::{debug, error, info};

type ClientId = String;
type MessageSender = Sender<Result<Message, MessagesError>>;
type MessageReceiver = Receiver<Result<Message, MessagesError>>;

pub static TEST_FAKE_SERVER: Lazy<DashMap<NatsConfig, Arc<FakeNatsServer>>> =
    Lazy::new(|| DashMap::new());

/// Represents a subscription to a set of subjects in the fake NATS server.
///
/// A `FakeSubscription` contains a unique subscriber ID and a list of subjects
/// that the subscriber is interested in. It provides functionality to check if
/// a given subject matches any of the filter subjects in the subscription.
///
/// # Fields
///
/// * `subscriber_id` - A unique identifier for the subscriber.
/// * `filter_subjects` - A list of subjects that the subscriber is interested in.
///
/// # Methods
///
/// * `match_subject` - Checks if a given subject matches any of the filter subjects in the subscription.
///
#[derive(Clone, Default, Debug, PartialEq, Eq, Hash)]
pub struct FakeSubscription {
    subscriber_id: u64,
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
/// the subject of the message.
///
/// # Fields
///
/// * `subscribers` - A map of client IDs to their respective subscriptions and message senders.
/// * `storage` - A mutex-protected vector of messages that have been published to the server.
/// * `subscription_ids` - An atomic counter for generating unique subscription IDs.
///
/// # Methods
///
/// * `new` - Creates a new instance of the fake NATS server.
/// * `add_subscription` - Adds a new subscription for a client with specified filter subjects and delivery policy.
/// * `publish_existing_messages` - Publishes existing messages to a subscriber based on their subscription.
/// * `remove_subscription` - Removes a subscription for a client.
/// * `publish` - Publishes a message to the server and distributes it to the appropriate subscribers.
/// * `persist_message` - Persists a message to the server's storage.
/// * `distribute_to_subscribers` - Distributes a message to the appropriate subscribers based on the subject.
///
pub struct FakeNatsServer {
    subscribers: DashMap<ClientId, DashMap<FakeSubscription, MessageSender>>,
    storage: Mutex<Vec<Message>>,
    subscription_ids: AtomicU64,
    connection_error: AtomicBool,
    failed_connection_attempts: AtomicU64,
}

impl FakeNatsServer {
    pub fn new() -> FakeNatsServer {
        FakeNatsServer {
            subscribers: DashMap::new(),
            subscription_ids: AtomicU64::new(1),
            storage: Mutex::new(vec![]),
            connection_error: AtomicBool::new(false),
            failed_connection_attempts: AtomicU64::new(0),
        }
    }

    pub fn get_by_config(config: &NatsConfig) -> Option<Arc<FakeNatsServer>> {
        TEST_FAKE_SERVER.get(config).map(|server| server.clone())
    }

    pub fn enable_connection_error(&self) {
        self.connection_error.store(true, Ordering::Release);
        self.subscribers.iter().for_each(|e| {
            e.value().iter().for_each(|e| {
                debug!("sending error to {:?}", e.key());
                if let Err(e) = e
                    .value()
                    .send(Err(MessagesErrorKind::MissingHeartbeat.into()))
                {
                    error!("error sending to channel {:?}", e);
                }
                e.value().close();
            });
        });
    }

    pub fn disable_connection_error(&self) {
        self.connection_error.store(false, Ordering::Release);
    }

    pub async fn wait_for_connection_error(&self, duration: std::time::Duration) -> Result<u64> {
        let failures = tokio::time::timeout(duration, async {
            while self.failed_connection_attempts.load(Ordering::Acquire) == 0 {
                yield_now().await;
            }
            let failed_attempts = self.failed_connection_attempts.load(Ordering::Acquire);
            self.failed_connection_attempts.store(0, Ordering::Release);
            failed_attempts
        })
        .await?;
        Ok(failures)
    }
}

impl FakeNatsServer {
    pub async fn add_subscription(
        &self,
        client_id: String,
        filter_subjects: Vec<Subject>,
        deliver_policy: DeliverPolicy,
    ) -> Result<(FakeSubscription, MessageReceiver)> {
        if self.connection_error.load(Ordering::Acquire) {
            self.failed_connection_attempts
                .fetch_add(1, Ordering::AcqRel);
            return Err(anyhow!("simulated connection error"));
        }
        let subscriber_id = self.subscription_ids.fetch_add(1, Ordering::AcqRel);

        info!(%client_id, ?subscriber_id, ?filter_subjects, "add message subscription to FakeNatsServer");

        let (subscriber_tx, subscriber_rx) = loole::unbounded::<Result<Message, MessagesError>>();
        let subscription = FakeSubscription {
            subscriber_id,
            filter_subjects,
        };

        match deliver_policy {
            DeliverPolicy::All => {
                self.publish_existing_messages(&subscription, subscriber_tx.clone())
                    .await
                    .context(
                        "FakeNatsServer: Publishing existing messages for delivery policy ALL",
                    )?;
            }
            DeliverPolicy::New => (),
            policy => bail!("FakeNatsServer: Unimplemented deliver policy {:?}", policy),
        }

        self.subscribers
            .entry(client_id)
            .or_insert_with(DashMap::new)
            .insert(subscription.clone(), subscriber_tx);

        Ok((subscription, subscriber_rx))
    }

    async fn publish_existing_messages(
        &self,
        subscription: &FakeSubscription,
        subscriber_tx: Sender<Result<Message, MessagesError>>,
    ) -> Result<()> {
        let storage = self.storage.lock().await;
        for message in storage.iter() {
            let subject: rhio_core::Subject =
                rhio_core::Subject::from_str(message.subject.as_str())?;
            if subscription.match_subject(&subject) {
                subscriber_tx
                    .send(Ok(message.clone()))
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

    pub async fn publish(&self, subject: Subject, message: Message) -> Result<()> {
        self.persist_message(&message).await;
        self.distribute_to_subscribers(subject, message)
    }

    async fn persist_message(&self, message: &Message) {
        let mut storage = self.storage.lock().await;
        storage.push(message.clone());
    }

    fn distribute_to_subscribers(&self, subject: Subject, message: Message) -> Result<()> {
        for subscription_entry in self.subscribers.iter() {
            let subscriptions = subscription_entry.value();

            for subscription in subscriptions.iter() {
                if subscription.key().match_subject(&subject) {
                    subscription
                        .value()
                        .send(Ok(message.clone()))
                        .context(format!(
                            "FakeNatsClient: Send message to subscriber {:?}",
                            subscription.key()
                        ))?;
                }
            }
        }
        Ok(())
    }
}
