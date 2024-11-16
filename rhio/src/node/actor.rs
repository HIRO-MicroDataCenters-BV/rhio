use anyhow::{anyhow, bail, Context, Result};
use async_nats::jetstream::consumer::DeliverPolicy;
use async_nats::Message as NatsMessage;
use futures_util::stream::SelectAll;
use p2panda_core::PrivateKey;
use p2panda_net::network::FromNetwork;
use p2panda_net::TopicId;
use rhio_blobs::{BlobHash, BucketName, ObjectKey, ObjectSize};
use rhio_core::message::NetworkPayload;
use rhio_core::{NetworkMessage, ScopedBucket, ScopedSubject};
use s3::error::S3Error;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;
use tracing::{debug, error, warn};

use crate::blobs::watcher::{S3Event, S3Watcher};
use crate::blobs::Blobs;
use crate::nats::{JetStreamEvent, Nats};
use crate::network::Panda;
use crate::node::Publication;
use crate::topic::{Query, Subscription};

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
        // shutdown completed to signal
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
        self.publications.push(publication.clone());

        // 1. Subscribe to p2panda gossip overlay for "live-mode".
        //
        // @TODO(adz): Doing this via this `NoSync` option is a hacky workaround. See sync
        // implementation for more details.
        let topic_query = match &publication {
            Publication::Bucket { bucket } => Query::NoSyncBucket {
                public_key: bucket.public_key(),
            },
            Publication::Subject { subject, .. } => Query::NoSyncSubject {
                public_key: subject.public_key(),
            },
        };

        let topic_id = topic_query.id();
        let network_rx = self.panda.subscribe(topic_query).await?;

        // Wrap broadcast receiver stream into tokio helper, to make it implement the `Stream`
        // trait which is required by `SelectAll`.
        self.p2panda_topic_rx.push(BroadcastStream::new(network_rx));

        // 2. Subscribe to an external data source for newly incoming data, so we can forward it to
        //    the gossip overlay later.
        match publication {
            Publication::Bucket { bucket: _ } => {
                // Do nothing here. We handle incoming new blob events via the "on_watcher_event"
                // method.
            }
            Publication::Subject {
                stream_name,
                subject,
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

    /// Application decided to subscribe to a new NATS message stream or S3 bucket.
    ///
    /// When subscribing we both subscribe to the gossip overlay and look for peers we can initiate
    /// sync sessions with (to catch up on past data).
    async fn on_subscribe(&mut self, subscription: Subscription) -> Result<()> {
        self.subscriptions.push(subscription.clone());
        let network_rx = self.panda.subscribe(subscription.into()).await?;
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
    async fn on_nats_message(
        &mut self,
        is_init: bool,
        topic_id: [u8; 32],
        message: NatsMessage,
    ) -> Result<()> {
        // Ignore messages when they're from the past, at this point we're only forwarding new
        // messages.
        if is_init {
            return Ok(());
        }

        debug!(subject = %message.subject, "received nats message, broadcast it in gossip overlay");
        let network_message = NetworkMessage::new_nats(message);
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
                debug!(
                    source = "gossip",
                    bytes = bytes.len(),
                    delivered_from = %delivered_from,
                    "received network message"
                );
                (bytes, delivered_from)
            }
            FromNetwork::SyncMessage {
                header,
                delivered_from,
                ..
            } => {
                debug!(
                    source = "sync",
                    bytes = header.len(),
                    delivered_from = %delivered_from,
                    "received network message"
                );
                (header, delivered_from)
            }
        };

        let network_message = NetworkMessage::from_bytes(&bytes)?;

        // Make sure the message comes from the same peer.
        if !network_message.verify(&delivered_from) {
            warn!(node_id = %delivered_from, "ignored network message with invalid signature");
            return Ok(());
        }

        match &network_message.payload {
            NetworkPayload::BlobAnnouncement(hash, bucket, key, size) => {
                // Make sure the bucket comes from the same peer.
                if !network_message.verify(&bucket.public_key()) {
                    warn!(node_id = %delivered_from, "ignored blob announcement with invalid owner");
                    return Ok(());
                }

                // We're interested in a bucket from a _specific_ public key. Filter out everything
                // which is not the right bucket name or not the right author.
                if is_bucket_matching(&self.subscriptions, bucket) {
                    self.blobs
                        .download(*hash, bucket.bucket_name(), key.to_owned(), *size)
                        .await?;
                }
            }
            NetworkPayload::NatsMessage(message) => {
                // Filter out all incoming messages we're not subscribed to. This can happen
                // especially when receiving messages over the gossip overlay as they are not
                // necessarily for us.
                let subject: ScopedSubject = message.subject.clone().try_into()?;

                // Make sure the NATS message comes from the same peer.
                if !network_message.verify(&subject.public_key()) {
                    warn!(node_id = %delivered_from, "ignored NATS message with invalid owner");
                    return Ok(());
                }

                if !is_subject_matching(&self.subscriptions, &subject) {
                    return Ok(());
                }

                self.nats
                    .publish(
                        true,
                        message.subject.to_string(),
                        message.headers.clone(),
                        message.payload.to_vec(),
                    )
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
        let Some(publication) = is_bucket_publishable(&self.publications, &bucket_name) else {
            return Ok(());
        };
        let Publication::Bucket { bucket } = publication else {
            unreachable!("method will always return a bucket publication");
        };

        let topic_id = Query::from(publication.to_owned()).id();
        let network_message =
            NetworkMessage::new_blob_announcement(hash, bucket.to_owned(), key, size);
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

    /// Sign network message and broadcast it in gossip overlay for this topic.
    async fn broadcast(&self, mut message: NetworkMessage, topic_id: [u8; 32]) -> Result<()> {
        message.sign(&self.private_key);
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

/// Returns true if incoming NATS message is of interested to our local node.
fn is_subject_matching(subscriptions: &Vec<Subscription>, incoming: &ScopedSubject) -> bool {
    for subscription in subscriptions {
        match subscription {
            Subscription::Bucket { .. } => continue,
            Subscription::Subject { subject, .. } => {
                if subject.is_matching(incoming) {
                    return true;
                } else {
                    continue;
                }
            }
        }
    }
    false
}

/// Returns true if incoming blob announcement is of interested to our local node.
fn is_bucket_matching(subscriptions: &Vec<Subscription>, incoming: &ScopedBucket) -> bool {
    for subscription in subscriptions {
        match subscription {
            Subscription::Bucket { bucket } => {
                if bucket == incoming {
                    return true;
                } else {
                    continue;
                }
            }
            Subscription::Subject { .. } => {
                continue;
            }
        }
    }
    false
}

/// Returns the public key for the blob announcement if we're okay with publishing it.
fn is_bucket_publishable<'a>(
    publications: &'a Vec<Publication>,
    bucket_name: &BucketName,
) -> Option<&'a Publication> {
    for publication in publications {
        match publication {
            Publication::Bucket { bucket } => {
                if &bucket.bucket_name() == bucket_name {
                    return Some(publication);
                } else {
                    continue;
                }
            }
            Publication::Subject { .. } => {
                continue;
            }
        }
    }
    None
}
