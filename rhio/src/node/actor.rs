use anyhow::{anyhow, bail, Context, Result};
use async_nats::jetstream::consumer::DeliverPolicy;
use async_nats::{HeaderMap, Message as NatsMessage};
use futures_util::stream::SelectAll;
use p2panda_core::{PrivateKey, PublicKey};
use p2panda_net::network::FromNetwork;
use p2panda_net::TopicId;
use rhio_blobs::{BlobHash, BucketName, ObjectKey, ObjectSize};
use rhio_core::{NetworkMessage, NetworkPayload, Subject, NATS_RHIO_PUBLIC_KEY, NATS_RHIO_SIGNATURE};
use s3::error::S3Error;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;
use tracing::{debug, error, trace, warn};

use crate::blobs::watcher::{S3Event, S3Watcher};
use crate::blobs::Blobs;
use crate::nats::{JetStreamEvent, Nats};
use crate::network::Panda;
use crate::node::Publication;
use crate::topic::{
    is_bucket_matching, is_bucket_publishable, is_subject_matching, Query, Subscription,
};

pub enum ToNodeActor {
    Publish {
        publication: Publication,
        reply: oneshot::Sender<Result<()>>,
    },
    Subscribe {
        subscription: Subscription,
        reply: oneshot::Sender<Result<()>>,
    },
    Shutdown {
        reply: oneshot::Sender<()>,
    },
}

pub struct NodeActor {
    private_key: PrivateKey,
    public_key: PublicKey,
    inbox: mpsc::Receiver<ToNodeActor>,
    subscriptions: Vec<Subscription>,
    publications: Vec<Publication>,
    nats_consumer_rx: SelectAll<BroadcastStream<JetStreamEvent>>,
    p2panda_topic_rx: SelectAll<BroadcastStream<FromNetwork>>,
    s3_watcher_rx: mpsc::Receiver<Result<S3Event, S3Error>>,
    nats: Nats,
    panda: Panda,
    blobs: Blobs,
    #[allow(dead_code)]
    watcher: S3Watcher,
}

impl NodeActor {
    pub fn new(
        private_key: PrivateKey,
        nats: Nats,
        panda: Panda,
        blobs: Blobs,
        watcher: S3Watcher,
        inbox: mpsc::Receiver<ToNodeActor>,
        s3_watcher_rx: mpsc::Receiver<Result<S3Event, S3Error>>,
    ) -> Self {
        Self {
            public_key: private_key.public_key(),
            private_key,
            inbox,
            subscriptions: Vec::new(),
            publications: Vec::new(),
            nats_consumer_rx: SelectAll::new(),
            p2panda_topic_rx: SelectAll::new(),
            s3_watcher_rx,
            nats,
            panda,
            blobs,
            watcher,
        }
    }

    pub async fn run(mut self) -> Result<()> {
        // Take oneshot sender from external API awaited by `shutdown` call and fire it as soon as
        // shutdown completed to signal.
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
                Some(msg) = self.inbox.recv() => {
                    match msg {
                        ToNodeActor::Shutdown { reply } => {
                            break Ok(reply);
                        }
                        msg => {
                            if let Err(err) = self.on_actor_message(msg).await {
                                break Err(err);
                            }
                        }
                    }
                },
                Some(Ok(event)) = self.nats_consumer_rx.next() => {
                    if let Err(err) = self.on_nats_event(event).await {
                        break Err(err);
                    }
                },
                Some(Ok(event)) = self.p2panda_topic_rx.next() => {
                    if let Err(err) = self.on_network_event(event).await {
                        break Err(err);
                    }
                },
                Some(event) = self.s3_watcher_rx.recv() => {
                    match event {
                        Ok(event) => {
                            if let Err(err) = self.on_watcher_event(event).await {
                                break Err(err);
                            }
                        },
                        Err(err) => break Err(anyhow!(err)),
                    }
                },
                else => {
                    // Error occurred outside of actor and our select! loop got disabled. We exit
                    // here with an error which will probably be overriden by the external error
                    // which caused the problem in first hand.
                    break Err(anyhow!("all select! branches are disabled"));
                }
            }
        }
    }

    async fn on_actor_message(&mut self, msg: ToNodeActor) -> Result<()> {
        match msg {
            ToNodeActor::Publish { publication, reply } => {
                let result = self.on_publish(publication).await;
                reply.send(result).ok();
            }
            ToNodeActor::Subscribe {
                subscription,
                reply,
            } => {
                let result = self.on_subscribe(subscription).await;
                reply.send(result).ok();
            }
            ToNodeActor::Shutdown { .. } => {
                unreachable!("handled in run_inner");
            }
        }

        Ok(())
    }

    /// Application decided to publish a local NATS message stream or S3 bucket.
    ///
    /// When publishing we don't want to sync but only gossip. Only subscribing peers will want to
    /// initiate sync sessions with us, as publishing peers we're only _accepting_ these sync
    /// sessions.
    async fn on_publish(&mut self, publication: Publication) -> Result<()> {
        // Configuration sanity checks.
        validate_publication_config(&self.publications, &publication)?;

        self.publications.push(publication.clone());

        // 1. Subscribe to p2panda gossip overlay for "live-mode".
        //
        // @TODO(adz): Doing this via this `NoSync` option is a hacky workaround. See sync
        // implementation for more details.
        let topic_query = match &publication {
            Publication::Bucket { public_key, .. } => Query::NoSyncBucket {
                public_key: *public_key,
            },
            Publication::Subject { public_key, .. } => Query::NoSyncSubject {
                public_key: *public_key,
            },
        };

        let topic_id = topic_query.id();

        // This method returns `None` if we're already subscribed to the same gossip overlay for
        // publications. We only need to do that once.
        let network_rx = self.panda.subscribe(topic_query).await?;

        // Wrap broadcast receiver stream into tokio helper, to make it implement the `Stream`
        // trait which is required by `SelectAll`.
        if let Some(network_rx) = network_rx {
            self.p2panda_topic_rx.push(BroadcastStream::new(network_rx));
        }

        // 2. Subscribe to an external data source for newly incoming data, so we can forward it to
        //    the gossip overlay later.
        match publication {
            Publication::Bucket { .. } => {
                // Do nothing here. We handle incoming new blob events via the "on_watcher_event"
                // method.
            }
            Publication::Subject {
                stream_name,
                subject,
                ..
            } => {
                // Subscribe to the NATS stream to receive new NATS messages from here.
                let (_, nats_rx) = self
                    .nats
                    .subscribe(stream_name, subject, DeliverPolicy::New, topic_id)
                    .await?;
                self.nats_consumer_rx.push(BroadcastStream::new(nats_rx));
            }
        };

        Ok(())
    }

    /// Application decided to subscribe to NATS messages or blobs from an author.
    ///
    /// When subscribing we both subscribe to the gossip overlay and look for peers we can initiate
    /// sync sessions with (to catch up on past data).
    async fn on_subscribe(&mut self, subscription: Subscription) -> Result<()> {
        // Configuration sanity checks.
        validate_subscription_config(&self.publications, &self.subscriptions, &subscription)?;

        self.subscriptions.push(subscription.clone());

        let network_rx = self
            .panda
            .subscribe(subscription.into())
            .await?
            .expect("queries for subscriptions should always return channel");
        self.p2panda_topic_rx.push(BroadcastStream::new(network_rx));

        Ok(())
    }

    /// Handler for incoming events from the NATS stream consumer.
    async fn on_nats_event(&mut self, event: JetStreamEvent) -> Result<()> {
        match event {
            JetStreamEvent::Message {
                topic_id,
                message,
                is_init,
            } => {
                self.on_nats_message(is_init, topic_id, message).await?;
            }
            JetStreamEvent::Failed {
                stream_name,
                reason,
                ..
            } => {
                bail!("stream '{}' failed: {}", stream_name, reason);
            }
            JetStreamEvent::InitCompleted { .. } => {
                // We do not handle sync sessions here which download all past messages first
                // ("initialization"). This event get's anyhow called. This is why we're simply
                // just ignoring it.
            }
        }

        Ok(())
    }

    /// Handler for incoming messages from the NATS JetStream consumer.
    ///
    /// From here we're broadcasting the NATS messages in the related gossip overlay network.
    ///
    /// These consumers have been set up based on our publication configuration, we can be sure
    /// that we _want_ to publish the messages coming via this channel, no further checks are
    /// required.
    async fn on_nats_message(
        &mut self,
        is_init: bool,
        topic_id: [u8; 32],
        message: NatsMessage,
    ) -> Result<()> {
        // Sanity check.
        assert!(
            !is_init,
            "we should never receive old NATS messages on this channel"
        );

        // Ignore messages which contain our custom "rhio" header to prevent broadcasting messages
        // right after we've ingested them on the same stream.
        //
        // This can happen if there's an overlap in subject filters, depending on the publish and
        // subscribe config.
        if let Some(headers) = &message.headers {
            if headers.get(NATS_RHIO_SIGNATURE).is_some() || headers.get(NATS_RHIO_PUBLIC_KEY).is_some() {
                return Ok(());
            }
        }

        debug!(subject = %message.subject, "received nats message, broadcast it in gossip overlay");

        // Sign message as it definitely comes from us at this point (it doesn't have any signature
        // or public key yet).
        let mut network_message = NetworkMessage::new_nats(message, &self.public_key);
        network_message.sign(&self.private_key);

        self.broadcast(network_message, topic_id).await?;

        Ok(())
    }

    /// Handler for incoming events from the p2p network.
    ///
    /// These events can come from either gossip broadcast or sync sessions with other peers.
    async fn on_network_event(&mut self, event: FromNetwork) -> Result<()> {
        let (bytes, delivered_from) = match event {
            FromNetwork::GossipMessage {
                bytes,
                delivered_from,
            } => {
                trace!(
                    source = "gossip",
                    bytes = bytes.len(),
                    "received network message"
                );
                (bytes, delivered_from)
            }
            FromNetwork::SyncMessage {
                header,
                delivered_from,
                ..
            } => {
                trace!(
                    source = "sync",
                    bytes = header.len(),
                    "received network message"
                );
                (header, delivered_from)
            }
        };

        let network_message = NetworkMessage::from_bytes(&bytes)?;

        // Check the signature of the blob announcement or NATS message.
        if !network_message.verify() {
            warn!(
                %delivered_from, public_key = %network_message.public_key,
                "ignored network message with invalid signature"
            );
            return Ok(());
        }

        match &network_message.payload {
            NetworkPayload::BlobAnnouncement(hash, key, size) => {
                debug!(%hash, %key, %size, "received blob announcement");

                // We're interested in blobs from a _specific_ public key. Filter out everything
                // which is _not_ the right author.
                if let Some(Subscription::Bucket { bucket_name, .. }) =
                    is_bucket_matching(&self.subscriptions, &network_message.public_key)
                {
                    self.blobs
                        .download(*hash, bucket_name.to_owned(), key.to_owned(), *size)
                        .await?;
                }
            }
            NetworkPayload::NatsMessage(subject, payload, headers) => {
                debug!(%subject, ?payload, "received NATS message");

                // We're interested in NATS messages from a _specific_ public key and matching
                // subject (with wildcard support).
                //
                // Filter out all incoming messages we're not subscribed to. This can happen
                // especially when receiving messages over the gossip overlay as they are not
                // necessarily for us.
                let subject: Subject = subject.parse()?;
                if !is_subject_matching(&self.subscriptions, &subject, &network_message.public_key)
                {
                    return Ok(());
                }

                // @TODO: Revisit this ..
                // We're adding a custom rhio header to the NATS message, to mark this message as
                // "ingested" by rhio. This helps us to identify messages which already have been
                // processed by us, so we don't need to send them again when they arrive at a NATS
                // consumer for gossip broadcast.
                let headers = match &headers {
                    Some(headers) => headers.clone(),
                    None => HeaderMap::new(),
                };
                // let timestamp = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH)?;
                // headers.insert(NATS_FROM_RHIO_HEADER, timestamp.as_secs().to_string());

                self.nats
                    .publish(true, subject.to_string(), Some(headers), payload.to_vec())
                    .await?;
            }
        }

        Ok(())
    }

    /// Handler for incoming events from the S3 watcher service.
    ///
    /// This service informs us about state changes in the S3 database (regular S3-compatible
    /// database) or blob store (used for storing data required to do p2p blob sync, this data is
    /// also stored in S3 database next to the actual synced objects).
    async fn on_watcher_event(&self, event: S3Event) -> Result<()> {
        match event {
            S3Event::DetectedS3Object(bucket_name, key, size) => {
                self.on_detected_s3_object(bucket_name, key, size).await?;
            }
            S3Event::BlobImportFinished(hash, bucket_name, key, size) => {
                self.on_import_finished(hash, bucket_name, key, size)
                    .await?;
            }
            S3Event::DetectedIncompleteBlob(hash, bucket_name, key, size) => {
                self.on_incomplete_blob_detected(hash, bucket_name, key, size)
                    .await?;
            }
        }

        Ok(())
    }

    /// Handler when user uploaded a new object directly into the S3 bucket.
    async fn on_detected_s3_object(
        &self,
        bucket_name: BucketName,
        key: ObjectKey,
        size: ObjectSize,
    ) -> Result<()> {
        // Import the object into our blob store (generate a bao-encoding and make it ready for p2p
        // sync).
        self.blobs.import_s3_object(bucket_name, key, size).await?;
        Ok(())
    }

    /// Handler when blob store finished importing new S3 object.
    async fn on_import_finished(
        &self,
        hash: BlobHash,
        bucket_name: BucketName,
        key: ObjectKey,
        size: ObjectSize,
    ) -> Result<()> {
        // @TODO: Revisit this .. we actually support also publishing other author's blobs!
        // Make sure we're allowing publishing from this bucket.
        let Some(Publication::Bucket {
            bucket_name,
            public_key,
        }) = is_bucket_publishable(&self.publications, &bucket_name)
        else {
            return Ok(());
        };

        debug!(%hash, %bucket_name, %key, %size, "broadcast blob announcement");

        let topic_id = Query::Bucket {
            // @TODO: Revisit this ..
            public_key: *public_key,
        }
        .id();

        // @TODO: Sign message when blob is coming from us, otherwise just forward the signature
        // and public key.
        let network_message =
            NetworkMessage::new_blob_announcement(hash, key, size, &self.public_key);
        self.broadcast(network_message, topic_id).await?;

        Ok(())
    }

    /// Handler when incomplete blob was detected, probably the process was exited before the
    /// download hash finished.
    async fn on_incomplete_blob_detected(
        &self,
        hash: BlobHash,
        bucket_name: BucketName,
        key: ObjectKey,
        size: ObjectSize,
    ) -> Result<()> {
        self.blobs.download(hash, bucket_name, key, size).await?;
        Ok(())
    }

    /// Broadcast message in gossip overlay for this topic.
    async fn broadcast(&self, message: NetworkMessage, topic_id: [u8; 32]) -> Result<()> {
        self.panda
            .broadcast(message.to_bytes(), topic_id)
            .await
            .context("broadcast message")?;

        Ok(())
    }

    async fn shutdown(&self) -> Result<()> {
        self.nats.shutdown().await?;
        self.panda.shutdown().await?;
        self.blobs.shutdown().await?;
        Ok(())
    }
}

fn validate_publication_config(
    publications: &Vec<Publication>,
    publication: &Publication,
) -> Result<()> {
    // 1. Published bucket names need to be unique.
    // 2. Published NATS subject + stream name tuples need to be unique.
    for existing_publication in publications {
        match &publication {
            Publication::Bucket { bucket_name, .. } => match existing_publication {
                Publication::Bucket {
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
                Publication::Subject { .. } => {
                    continue;
                }
            },
            Publication::Subject {
                subject,
                stream_name,
                ..
            } => match existing_publication {
                Publication::Bucket { .. } => continue,
                Publication::Subject {
                    subject: existing_subject,
                    stream_name: existing_stream_name,
                    ..
                } => {
                    if existing_subject == subject && existing_stream_name == stream_name {
                        bail!(
                            "publish config contains duplicate NATS subject {} and stream {}",
                            subject,
                            stream_name
                        );
                    }
                }
            },
        }
    }

    Ok(())
}

fn validate_subscription_config(
    publications: &Vec<Publication>,
    subscriptions: &Vec<Subscription>,
    subscription: &Subscription,
) -> Result<()> {
    // 1. Subscribed bucket names can't be used for publishing as well.
    for existing_publication in publications {
        match &subscription {
            Subscription::Bucket { bucket_name, .. } => match existing_publication {
                Publication::Bucket {
                    bucket_name: existing_bucket_name,
                    ..
                } => {
                    if existing_bucket_name == bucket_name {
                        bail!("bucket {} is already used in publish config", bucket_name);
                    }
                }
                Publication::Subject { .. } => {
                    continue;
                }
            },
            Subscription::Subject { .. } => {
                continue;
            }
        }
    }

    // 2. Subscribed public key can't be re-used for another bucket.
    // 3. Subscribed NATS subject + stream name tuples need to be unique.
    for existing_subscribtion in subscriptions {
        match &subscription {
            Subscription::Bucket { public_key, .. } => match existing_subscribtion {
                Subscription::Bucket {
                    public_key: existing_public_key,
                    ..
                } => {
                    if existing_public_key == public_key {
                        bail!(
                            "public key {} is used multiple times in subscribe S3 config",
                            public_key
                        );
                    }
                }
                Subscription::Subject { .. } => {
                    continue;
                }
            },
            Subscription::Subject {
                public_key,
                subject,
                ..
            } => match existing_subscribtion {
                Subscription::Bucket { .. } => continue,
                Subscription::Subject {
                    subject: existing_subject,
                    public_key: existing_public_key,
                    ..
                } => {
                    if public_key == existing_public_key && subject == existing_subject {
                        bail!(
                            "public key {} and subject {} is used multiple times in subscribe NATS config",
                            public_key, 
                            subject
                        );
                    }
                }
            },
        }
    }

    Ok(())
}
