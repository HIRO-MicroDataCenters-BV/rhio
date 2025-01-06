use anyhow::{anyhow, bail, Context, Result};
use async_nats::jetstream::consumer::DeliverPolicy;
use async_nats::Message as NatsMessage;
use futures_util::stream::SelectAll;
use loole::RecvStream;
use p2panda_core::{PrivateKey, PublicKey};
use p2panda_net::network::FromNetwork;
use p2panda_net::TopicId;
use rhio_blobs::{CompletedBlob, NotImportedObject, SignedBlobInfo};
use rhio_core::{nats, NetworkMessage, NetworkPayload, Subject};
use s3::error::S3Error;
use tokio::sync::{mpsc, oneshot};
use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::StreamExt;
use tracing::{debug, error, trace, warn};

use crate::blobs::watcher::S3Event;
use crate::blobs::Blobs;
use crate::nats::{JetStreamEvent, Nats};
use crate::network::Panda;
use crate::node::config::NodeConfig;
use crate::topic::{Query, Subscription};
use crate::Publication;

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
    config: NodeConfig,
    private_key: PrivateKey,
    public_key: PublicKey,
    inbox: mpsc::Receiver<ToNodeActor>,
    nats_consumer_rx: SelectAll<RecvStream<JetStreamEvent>>,
    p2panda_topic_rx: SelectAll<ReceiverStream<FromNetwork>>,
    s3_watcher_rx: mpsc::Receiver<Result<S3Event, S3Error>>,
    nats: Nats,
    panda: Panda,
    blobs: Option<Blobs>,
}

impl NodeActor {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        config: NodeConfig,
        private_key: PrivateKey,
        nats: Nats,
        panda: Panda,
        blobs: Option<Blobs>,
        inbox: mpsc::Receiver<ToNodeActor>,
        s3_watcher_rx: mpsc::Receiver<Result<S3Event, S3Error>>,
    ) -> Self {
        Self {
            config,
            public_key: private_key.public_key(),
            private_key,
            inbox,
            nats_consumer_rx: SelectAll::new(),
            p2panda_topic_rx: SelectAll::new(),
            s3_watcher_rx,
            nats,
            panda,
            blobs,
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
                Some(event) = self.nats_consumer_rx.next() => {
                    if let Err(err) = self.on_nats_event(event).await {
                        break Err(err);
                    }
                },
                Some(event) = self.p2panda_topic_rx.next() => {
                    self.on_network_event(event).await?;
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
        self.config.add_publication(&publication).await?;

        // 1. Subscribe to p2panda gossip overlay for "live-mode".
        //
        // @TODO(adz): Doing this via this `NoSync` option is a hacky workaround. See sync
        // implementation for more details.
        let topic_query = match &publication {
            Publication::Files { public_key, .. } => Query::NoSyncFiles {
                public_key: *public_key,
            },
            Publication::Messages { public_key, .. } => Query::NoSyncMessages {
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
            self.p2panda_topic_rx.push(ReceiverStream::new(network_rx));
        }

        // 2. Subscribe to an external data source for newly incoming data, so we can forward it to
        //    the gossip overlay later.
        match publication {
            Publication::Files { .. } => {
                // Do nothing here. We handle incoming new blob events via the "on_watcher_event"
                // method.
            }
            Publication::Messages {
                filtered_stream, ..
            } => {
                // Subscribe to the NATS stream to receive new NATS messages from here.
                let (_, nats_rx) = self
                    .nats
                    .subscribe(
                        filtered_stream.stream_name,
                        filtered_stream.subjects,
                        DeliverPolicy::New,
                        topic_id,
                    )
                    .await?;
                self.nats_consumer_rx.push(nats_rx.into_stream());
            }
        };

        Ok(())
    }

    /// Application decided to subscribe to NATS messages or S3 objects from an author's bucket.
    ///
    /// When subscribing we both subscribe to the gossip overlay and look for peers we can initiate
    /// sync sessions with (to catch up on past data).
    async fn on_subscribe(&mut self, subscription: Subscription) -> Result<()> {
        self.config.add_subscription(&subscription).await?;

        let network_rx = self
            .panda
            .subscribe(subscription.into())
            .await?
            .expect("queries for subscriptions should always return channel");
        self.p2panda_topic_rx.push(ReceiverStream::new(network_rx));

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
                // Sanity check.
                assert!(
                    !is_init,
                    "we should never receive old NATS messages on this channel"
                );

                self.on_nats_message(topic_id, message).await?;
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
    async fn on_nats_message(&mut self, topic_id: [u8; 32], message: NatsMessage) -> Result<()> {
        // Ignore messages which contain our custom "rhio" header to prevent broadcasting messages
        // right after we've ingested them on the same stream.
        //
        // This can happen if there's an overlap in subject filters, depending on the publish and
        // subscribe config.
        if nats::has_nats_signature(&message.headers) {
            return Ok(());
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
        let (bytes, delivered_from, is_gossip) = match event {
            FromNetwork::GossipMessage {
                bytes,
                delivered_from,
            } => {
                trace!(
                    source = "gossip",
                    bytes = bytes.len(),
                    "received network message"
                );
                (bytes, delivered_from, true)
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
                (header, delivered_from, false)
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

        let signature = network_message
            .signature
            .expect("signatures was already checked at this point and should be given");

        match &network_message.payload {
            NetworkPayload::BlobAnnouncement(hash, remote_bucket_name, key, size) => {
                if is_gossip {
                    trace!(%hash, %key, %size, "ignoring blob announcement received via gossip");
                    return Ok(());
                }

                if self.blobs.is_none() {
                    trace!(%hash, %key, %size, "ignoring blob announcement since blobs are not configured");
                    return Ok(());
                }

                debug!(%hash, %key, %size, "received blob announcement");

                // We're interested in blobs from a _specific_ public key and bucket. Filter out
                // everything which is _not_ the right one.
                if let Some(local_bucket_name) = self
                    .config
                    .is_files_subscription_matching(&network_message.public_key, remote_bucket_name)
                    .await
                {
                    self.blobs
                        .as_ref()
                        .unwrap()
                        .download(SignedBlobInfo {
                            hash: *hash,
                            local_bucket_name,
                            remote_bucket_name: remote_bucket_name.clone(),
                            key: key.clone(),
                            size: *size,
                            public_key: network_message.public_key,
                            signature,
                        })
                        .await?;
                }
            }
            NetworkPayload::NatsMessage(subject, payload, previous_headers) => {
                // We're interested in NATS messages from a _specific_ public key and matching
                // subject (with wildcard support).
                //
                // Filter out all incoming messages we're not subscribed to. This can happen
                // especially when receiving messages over the gossip overlay as they are not
                // necessarily for us.
                let subject: Subject = subject.parse()?;
                if !self
                    .config
                    .is_subject_subscription_matching(&subject, &network_message.public_key)
                    .await
                {
                    return Ok(());
                }

                // Move the authentication data into the NATS message itself, so it doesn't get
                // lost after storing it in the NATS server.
                let headers = nats::add_custom_nats_headers(
                    previous_headers,
                    signature,
                    network_message.public_key,
                );

                self.nats
                    .publish(true, subject.to_string(), Some(headers), payload.to_vec())
                    .await?;
            }
        }

        Ok(())
    }

    /// Handler for incoming events from the S3 watcher service.
    ///
    /// This service informs us about state changes in S3 buckets (regular S3-compatible database)
    /// or blob store (used for storing data required to do p2p blob sync, this data is also stored
    /// in S3 buckets next to the actual synced objects).
    async fn on_watcher_event(&self, event: S3Event) -> Result<()> {
        match event {
            S3Event::DetectedS3Object(object) => {
                self.on_detected_s3_object(object).await?;
            }
            S3Event::BlobImportFinished(completed_blob) => {
                self.on_import_finished(completed_blob).await?;
            }
            S3Event::DetectedIncompleteBlob(signed_blob) => {
                self.on_incomplete_blob_detected(signed_blob).await?;
            }
        }

        Ok(())
    }

    /// Handler when user uploaded a new object directly into a S3 bucket.
    async fn on_detected_s3_object(&self, object: NotImportedObject) -> Result<()> {
        // Import the object into our blob store (generate a bao-encoding and make it
        // ready for p2p sync) and sign it with our private key.
        if let Some(blobs) = self.blobs.as_ref() {
            blobs.import_s3_object(object).await?;
        }
        Ok(())
    }

    /// Handler when blob store finished importing new S3 object.
    async fn on_import_finished(&self, blob: CompletedBlob) -> Result<()> {
        // This method can be called from both importing new local S3 objects or downloading remote
        // blobs from other peers.
        //
        // We're only announcing blobs on the network we've uploaded ourselves locally (these are
        // unsigned).
        let CompletedBlob::Unsigned(blob) = blob else {
            return Ok(());
        };

        // Sanity: Make sure we're allowing publishing from this bucket. This should not be
        // necessary when the configuration's are sane, but we're checking it just to be sure.
        if !self
            .config
            .is_bucket_publishable(&blob.local_bucket_name)
            .await
        {
            warn!("tried to announce blob from an unpublishable S3 bucket");
            return Ok(());
        };

        debug!(
            hash = %blob.hash,
            bucket_name = %blob.local_bucket_name,
            key = %blob.key,
            size = %blob.size,
            "broadcast blob announcement"
        );

        // Announce the blob on the network and sign it with our key.
        let mut network_message = NetworkMessage::new_blob_announcement(
            blob.hash,
            blob.local_bucket_name.clone(),
            blob.key,
            blob.size,
            &self.public_key,
        );
        network_message.sign(&self.private_key);

        let topic_id = Query::Files {
            public_key: self.public_key,
            bucket_name: blob.local_bucket_name,
        }
        .id();
        self.broadcast(network_message, topic_id).await?;

        Ok(())
    }

    /// Handler when incomplete blob was detected, probably the process was exited before the
    /// download hash finished.
    async fn on_incomplete_blob_detected(&self, blob: SignedBlobInfo) -> Result<()> {
        if let Some(blobs) = self.blobs.as_ref() {
            blobs.download(blob).await?;
        }
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
        if let Some(blobs) = self.blobs.as_ref() {
            blobs.shutdown().await?;
        }
        Ok(())
    }
}
