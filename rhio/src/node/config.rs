use std::sync::Arc;

use anyhow::{bail, Result};
use p2panda_core::PublicKey;
use rhio_blobs::BucketName;
use rhio_core::Subject;
use tokio::sync::RwLock;

use crate::topic::{is_bucket_publishable, is_files_subscription_matching, is_subject_matching};
use crate::{FilteredMessageStream, MessagesSubscription, Publication, Subscription};

#[derive(Clone, Debug)]
pub struct NodeConfig {
    inner: Arc<RwLock<NodeConfigInner>>,
}

#[derive(Debug)]
struct NodeConfigInner {
    subscriptions: Vec<Subscription>,
    publications: Vec<Publication>,
}

impl NodeConfig {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(NodeConfigInner {
                subscriptions: Vec::new(),
                publications: Vec::new(),
            })),
        }
    }

    pub async fn message_publications(&self) -> Vec<FilteredMessageStream> {
        let inner = self.inner.read().await;
        inner
            .publications
            .iter()
            .filter_map(|publication| match publication {
                Publication::Files { .. } => None,
                Publication::Messages {
                    filtered_stream, ..
                } => Some(filtered_stream.clone()),
            })
            .collect()
    }

    pub async fn message_subscriptions(&self) -> Vec<MessagesSubscription> {
        let inner = self.inner.read().await;
        inner
            .subscriptions
            .iter()
            .filter_map(|subscription| match subscription {
                Subscription::Files(_) => None,
                Subscription::Messages(messages_subscription) => {
                    Some(messages_subscription.clone())
                }
            })
            .collect()
    }

    pub async fn add_publication(&mut self, publication: &Publication) -> Result<()> {
        self.validate_publication(publication).await?;

        let mut inner = self.inner.write().await;
        inner.publications.push(publication.clone());

        Ok(())
    }

    pub async fn add_subscription(&mut self, subscription: &Subscription) -> Result<()> {
        self.validate_subscription(subscription).await?;

        let mut inner = self.inner.write().await;
        inner.subscriptions.push(subscription.clone());

        Ok(())
    }

    pub async fn is_bucket_publishable(&self, bucket_name: &BucketName) -> bool {
        let existing = self.inner.read().await;
        is_bucket_publishable(&existing.publications, bucket_name).is_some()
    }

    pub async fn is_files_subscription_matching(
        &self,
        public_key: &PublicKey,
        bucket_name: &BucketName,
    ) -> Option<BucketName> {
        let existing = self.inner.read().await;
        match is_files_subscription_matching(&existing.subscriptions, public_key, bucket_name) {
            Some(Subscription::Files(subscription)) => Some(subscription.local_bucket_name.clone()),
            _ => None,
        }
    }

    pub async fn is_subject_subscription_matching(
        &self,
        subject: &Subject,
        public_key: &PublicKey,
    ) -> bool {
        let existing = self.inner.read().await;
        is_subject_matching(&existing.subscriptions, subject, public_key)
    }

    async fn validate_publication(&self, new_publication: &Publication) -> Result<()> {
        let existing = self.inner.read().await;

        // 1. Published bucket names need to be unique.
        // 2. Published NATS subject need to be unique.
        for existing_publication in &existing.publications {
            match &new_publication {
                Publication::Files { bucket_name, .. } => match existing_publication {
                    Publication::Files {
                        bucket_name: existing_bucket_name,
                        ..
                    } => {
                        if existing_bucket_name == bucket_name {
                            bail!(
                                "publish config contains duplicate S3 bucket {}",
                                bucket_name
                            );
                        }
                    }
                    Publication::Messages { .. } => {
                        continue;
                    }
                },
                Publication::Messages {
                    filtered_stream: new_filtered_stream,
                    ..
                } => match existing_publication {
                    Publication::Files { .. } => continue,
                    Publication::Messages {
                        filtered_stream: existing_filtered_stream,
                        ..
                    } => {
                        for new_subject in &new_filtered_stream.subjects {
                            if existing_filtered_stream.subjects.contains(new_subject) {
                                bail!(
                                    "publish config contains duplicate NATS subject '{}'",
                                    new_subject
                                );
                            }
                        }
                    }
                },
            }
        }

        Ok(())
    }

    async fn validate_subscription(&self, new_subscription: &Subscription) -> Result<()> {
        let existing = self.inner.read().await;

        // 1. Subscribed bucket names can't be used for publishing as well.
        for existing_publication in &existing.publications {
            match &new_subscription {
                Subscription::Files(new_files_subscription) => match existing_publication {
                    Publication::Files {
                        bucket_name: existing_bucket_name,
                        ..
                    } => {
                        if existing_bucket_name == &new_files_subscription.local_bucket_name {
                            bail!(
                            "local bucket '{}' for subscribe config is already used in publish config",
                            new_files_subscription.local_bucket_name.clone()
                        );
                        }
                    }
                    Publication::Messages { .. } => {
                        continue;
                    }
                },
                Subscription::Messages { .. } => {
                    continue;
                }
            }
        }

        // 2. Subscribed public key can't be re-used for another bucket.
        // 3. Subscribed NATS subject + public key tuples need to be unique.
        for existing_subscription in &existing.subscriptions {
            match &new_subscription {
                Subscription::Files(new_files_subscription) => match existing_subscription {
                    Subscription::Files(existing_files_subscription) => {
                        if existing_files_subscription.public_key
                            == new_files_subscription.public_key
                        {
                            bail!(
                                "public key {} is used multiple times in subscribe S3 config",
                                new_files_subscription.public_key
                            );
                        }
                    }
                    Subscription::Messages { .. } => {
                        continue;
                    }
                },
                Subscription::Messages(new_messages_subscription) => match existing_subscription {
                    Subscription::Files(_) => continue,
                    Subscription::Messages(existing_messages_subscription) => {
                        if new_messages_subscription.public_key
                            != existing_messages_subscription.public_key
                        {
                            continue;
                        }

                        for existing_stream in &existing_messages_subscription.filtered_streams {
                            for new_stream in &new_messages_subscription.filtered_streams {
                                for existing_subject in &existing_stream.subjects {
                                    if new_stream.subjects.contains(existing_subject) {
                                        bail!(
                                        "public key {} and subject '{}' is used multiple times in subscribe NATS config",
                                        existing_messages_subscription.public_key,
                                        existing_subject
                                    );
                                    }
                                }
                            }
                        }
                    }
                },
            }
        }

        Ok(())
    }
}
