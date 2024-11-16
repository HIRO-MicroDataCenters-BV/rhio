use std::sync::Arc;

use async_nats::jetstream::consumer::DeliverPolicy;
use async_nats::message::Message as NatsMessage;
use async_trait::async_trait;
use futures_util::future::{self};
use futures_util::stream::BoxStream;
use futures_util::{AsyncRead, AsyncWrite, Sink, SinkExt, StreamExt, TryStreamExt};
use p2panda_core::{Hash, PrivateKey, PublicKey, Signature};
use p2panda_net::TopicId;
use p2panda_sync::cbor::{into_cbor_sink, into_cbor_stream};
use p2panda_sync::{FromSync, SyncError, SyncProtocol};
use rand::random;
use rhio_blobs::{BlobHash, BucketName, ObjectSize, Paths, S3Store};
use rhio_core::{NetworkMessage, Subject};
use serde::{Deserialize, Serialize};
use tokio_stream::wrappers::BroadcastStream;
use tracing::{debug, span, Level};

use crate::config::Config;
use crate::nats::{ConsumerId, JetStreamEvent, Nats};
use crate::topic::Query;

type Challenge = u64;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Message {
    Handshake(Query, Challenge),
    HandshakeResponse(Signature),
    NatsHave(Vec<Hash>),
    NatsMessages(Vec<NetworkMessage>),
    BlobsHave(Vec<BlobHash>),
    Blobs(Vec<NetworkMessage>),
}

/// Simple sync protocol implementation to allow exchange of past NATS messages or blob
/// announcements.
// @TODO(adz): This implementation is sub-optimal as it requires the peers to send over
// _everything_ they know about. This can be optimized later with a smarter set reconciliation
// strategy though it'll be tricky to find out how to organize the data to make it more efficient
// (NATS messages do not have timestamps but we could sort them by sequential order of the filtered
// consumer, blobs could be sorted by S3 key (the absolute path)?).
#[derive(Clone, Debug)]
pub struct RhioSyncProtocol {
    config: Config,
    nats: Nats,
    blob_store: S3Store,
    private_key: PrivateKey,
}

#[async_trait]
impl<'a> SyncProtocol<'a, Query> for RhioSyncProtocol {
    fn name(&self) -> &'static str {
        "rhio-sync-v1"
    }

    async fn initiate(
        self: Arc<Self>,
        query: Query,
        tx: Box<&'a mut (dyn AsyncWrite + Send + Unpin)>,
        rx: Box<&'a mut (dyn AsyncRead + Send + Unpin)>,
        mut app_tx: Box<&'a mut (dyn Sink<FromSync<Query>, Error = SyncError> + Send + Unpin)>,
    ) -> Result<(), SyncError> {
        let span = span!(Level::DEBUG, "initiator", query = %query);

        let mut sink = into_cbor_sink(tx);
        let mut stream = into_cbor_stream(rx);

        // 1. Send sync query over to other peer so they'll learn what we want from them.
        debug!(parent: &span, "sending sync query {query}");
        app_tx
            .send(FromSync::HandshakeSuccess(query.clone()))
            .await?;
        let (message, challenge) = init_handshake(query.clone());
        sink.send(message).await?;

        // @TODO(adz): This is a workaround to disable syncing in some cases, for example when
        // we're a publishing peer we don't want to initiate syncing.
        //
        // The current p2panda API does not give any control to turn off syncing for some data
        // stream subscriptions, this is why we're doing it this hacky way.
        if matches!(query, Query::NoSyncSubject { .. })
            || matches!(query, Query::NoSyncBucket { .. })
        {
            debug!(parent: &span, "end sync session prematurely as we don't want to have one");
            return Ok(());
        }

        // 2. Verify that this peer is the one we're subscribed to.
        let message = stream.next().await.ok_or(SyncError::UnexpectedBehaviour(
            "incoming message stream ended prematurely".into(),
        ))??;
        let Message::HandshakeResponse(signature) = message else {
            return Err(SyncError::UnexpectedBehaviour(
                "did not receive expected message".into(),
            ));
        };

        if !verify_handshake(&signature, challenge, query.public_key()) {
            debug!(parent: &span, "end sync session as remote peer did not match our subscribed public keys");
            return Ok(());
        }

        // 3. We can sync over NATS messages or S3 blob announcements.
        match query {
            Query::Bucket {
                ref bucket_name, ..
            } => {
                // Send over a list of blob hashes we have already to remote peer.
                let blob_hashes: Vec<BlobHash> = self
                    .complete_blobs(&bucket_name)
                    .await
                    .iter()
                    .map(|(hash, _, _, _)| hash.to_owned())
                    .collect();
                debug!(parent: &span, "we have {} completed blobs", blob_hashes.len());
                sink.send(Message::BlobsHave(blob_hashes)).await?;

                // Wait for other peer to send us what we're missing.
                let message = stream.next().await.ok_or(SyncError::UnexpectedBehaviour(
                    "incoming message stream ended prematurely".into(),
                ))??;
                let Message::Blobs(remote_blob_announcements) = message else {
                    return Err(SyncError::UnexpectedBehaviour(
                        "did not receive expected message".into(),
                    ));
                };

                debug!(parent: &span,
                    "received {} new blob announcements from remote peer",
                    remote_blob_announcements.len()
                );

                for blob_announcement in remote_blob_announcements {
                    app_tx
                        .send(FromSync::Data(blob_announcement.to_bytes(), None))
                        .await?;
                }
            }
            Query::Subject { ref subject, .. } => {
                // NATS streams are configured locally for every peer, so we need to look it up
                // ourselves to find out what stream configuration we have for this subject.
                let stream_name = match &self.config.subscribe {
                    Some(subscriptions) => {
                        subscriptions.nats_subjects.iter().find_map(|subscription| {
                            if &subscription.subject == subject {
                                Some(subscription.stream_name.clone())
                            } else {
                                None
                            }
                        })
                    }
                    None => None,
                }
                .expect("query matches subscription config");

                // Download all NATS messages we have from the server for this subject and hash
                // them each. We send all hashes over to the other peer so they can determine and
                // send us what we don't have.
                let (consumer_id, nats_stream) =
                    self.nats_stream(stream_name, subject, query.id()).await?;
                let nats_stream = nats_stream.map(|event| match event {
                    Ok(message) => Ok(Hash::new(&message.payload)),
                    Err(err) => Err(err),
                });
                let nats_message_hashes: Result<Vec<Hash>, SyncError> =
                    nats_stream.try_collect().await;

                // Clean up consumer on error or success.
                self.unsubscribe_nats_stream(consumer_id).await?;
                let nats_message_hashes = nats_message_hashes?;

                debug!(parent: &span,
                    "downloaded {} NATS messages",
                    nats_message_hashes.len()
                );

                sink.send(Message::NatsHave(nats_message_hashes)).await?;

                // Wait for other peer to send us what we're missing.
                let message = stream.next().await.ok_or(SyncError::UnexpectedBehaviour(
                    "incoming message stream ended prematurely".into(),
                ))??;
                let Message::NatsMessages(nats_messages) = message else {
                    return Err(SyncError::UnexpectedBehaviour(
                        "did not receive expected message".into(),
                    ));
                };

                debug!(parent: &span,
                    "received {} new NATS messages from remote peer",
                    nats_messages.len()
                );

                for nats_message in nats_messages {
                    app_tx
                        .send(FromSync::Data(nats_message.to_bytes(), None))
                        .await?;
                }
            }
            Query::NoSyncBucket { .. } => {
                unreachable!("returned already earlier on no-sync option")
            }
            Query::NoSyncSubject { .. } => {
                unreachable!("returned already earlier on no-sync option")
            }
        }

        // Flush all bytes so that no messages are lost.
        sink.flush().await?;
        app_tx.flush().await?;

        debug!(parent: &span, "sync session finished");

        Ok(())
    }

    async fn accept(
        self: Arc<Self>,
        tx: Box<&'a mut (dyn AsyncWrite + Send + Unpin)>,
        rx: Box<&'a mut (dyn AsyncRead + Send + Unpin)>,
        mut app_tx: Box<&'a mut (dyn Sink<FromSync<Query>, Error = SyncError> + Send + Unpin)>,
    ) -> Result<(), SyncError> {
        let mut sink = into_cbor_sink(tx);
        let mut stream = into_cbor_stream(rx);

        // 1. Expect initiating peer to tell us what they want to sync.
        let message = stream.next().await.ok_or(SyncError::UnexpectedBehaviour(
            "incoming message stream ended prematurely".into(),
        ))??;
        let Message::Handshake(query, challenge) = message else {
            return Err(SyncError::UnexpectedBehaviour(
                "did not receive expected message".into(),
            ));
        };

        let span = span!(Level::DEBUG, "acceptor", query = %query);
        debug!(parent: &span, "received sync query {}", query);

        app_tx
            .send(FromSync::HandshakeSuccess(query.clone()))
            .await?;

        // The other peer might tell us sometimes that they _don't_ want to sync.
        //
        // @TODO(adz): This is a workaround to disable syncing in some cases as the current p2panda
        // API does not give any control to turn off syncing for some topics.
        if matches!(query, Query::NoSyncSubject { .. })
            || matches!(query, Query::NoSyncBucket { .. })
        {
            debug!(parent: &span, "end sync session prematurely as we don't want to have one");
            return Ok(());
        }

        // 2. Check if we are the peer the initiator is asking for, proof it if we are, otherwise
        //    send invalid proof and stop here.
        let accept_message = accept_handshake(challenge, &self.private_key);
        sink.send(accept_message).await?;

        if query.public_key() != &self.private_key.public_key() {
            debug!(parent: &span, "end sync session prematurely as we are not the peer the initiator asked for");
            return Ok(());
        }

        // 3. We can sync over NATS messages or S3 blob announcements.
        match &query {
            Query::Bucket { bucket_name, .. } => {
                // Await message from other peer on the blobs they _have_, so we can calculate what
                // they're missing and send that delta to them.
                let message = stream.next().await.ok_or(SyncError::UnexpectedBehaviour(
                    "incoming message stream ended prematurely".into(),
                ))??;
                let Message::BlobsHave(remote_blob_hashes) = message else {
                    return Err(SyncError::UnexpectedBehaviour(
                        "did not receive expected message".into(),
                    ));
                };

                let is_publishing = match &self.config.publish {
                    Some(publications) => publications
                        .s3_buckets
                        .iter()
                        .find(|local_bucket_name| &bucket_name == local_bucket_name),
                    None => None,
                }
                .is_some();

                if !is_publishing {
                    // Inform the other peer politely that we need to end here as we can't provide for
                    // this S3 bucket by sending them an empty array back.
                    debug!(parent: &span,
                        "can't provide data, politely send empty array back",
                    );
                    sink.send(Message::Blobs(vec![])).await?;
                    return Ok(());
                }

                let blob_announcements: Vec<NetworkMessage> = self
                    .complete_blobs(&bucket_name)
                    .await
                    .into_iter()
                    .filter_map(|(hash, _, paths, size)| {
                        if remote_blob_hashes.contains(&hash) {
                            None
                        } else {
                            Some({
                                let mut signed_msg = NetworkMessage::new_blob_announcement(
                                    hash,
                                    bucket_name.clone(),
                                    paths.data(),
                                    size,
                                );
                                signed_msg.sign(&self.private_key);
                                signed_msg
                            })
                        }
                    })
                    .collect();

                debug!(parent: &span, "send {} blob announcements", blob_announcements.len());

                sink.send(Message::Blobs(blob_announcements)).await?;
            }
            Query::Subject { subject, .. } => {
                // Look up our config to find out if we have a NATS stream somewhere which fits the
                // requested subject.
                let stream_name = match &self.config.publish {
                    Some(publications) => {
                        publications.nats_subjects.iter().find_map(|publication| {
                            if publication.subject.is_matching(subject) {
                                Some(publication.stream_name.clone())
                            } else {
                                None
                            }
                        })
                    }
                    None => None,
                };

                // Await message from other peer on the NATS messages they _have_, so we can
                // calculate what they're missing and send that delta to them.
                let message = stream.next().await.ok_or(SyncError::UnexpectedBehaviour(
                    "incoming message stream ended prematurely".into(),
                ))??;
                let Message::NatsHave(nats_message_hashes) = message else {
                    return Err(SyncError::UnexpectedBehaviour(
                        "did not receive expected message".into(),
                    ));
                };

                debug!(parent: &span,
                    "received {} hashes from remote peer",
                    nats_message_hashes.len()
                );

                // Inform the other peer politely that we need to end here as we can't provide for
                // this subject by sending them an empty array back.
                let Some(stream_name) = stream_name else {
                    debug!(parent: &span,
                        "can't provide data, politely send empty array back",
                    );
                    sink.send(Message::NatsMessages(vec![])).await?;
                    return Ok(());
                };

                let (consumer_id, nats_stream) =
                    self.nats_stream(stream_name, subject, query.id()).await?;
                let nats_stream = nats_stream.filter_map(|event| async {
                    match event {
                        Ok(message) => {
                            let hash = Hash::new(&message.payload);
                            if nats_message_hashes.contains(&hash) {
                                None
                            } else {
                                Some(Ok({
                                    let mut signed_msg = NetworkMessage::new_nats(message);
                                    signed_msg.sign(&self.private_key);
                                    signed_msg
                                }))
                            }
                        }
                        Err(err) => Some(Err(err)),
                    }
                });
                let nats_messages: Result<Vec<NetworkMessage>, SyncError> =
                    nats_stream.try_collect().await;

                // Clear up consumer after error or success.
                self.unsubscribe_nats_stream(consumer_id).await?;
                let nats_messages = nats_messages?;

                debug!(parent: &span, "downloaded {} NATS messages", nats_messages.len());

                sink.send(Message::NatsMessages(nats_messages)).await?;
            }
            Query::NoSyncBucket { .. } => {
                unreachable!("we've already returned before no-sync option")
            }
            Query::NoSyncSubject { .. } => {
                unreachable!("we've already returned before no-sync option")
            }
        }

        // Flush all bytes so that no messages are lost.
        sink.flush().await?;
        app_tx.flush().await?;

        debug!(parent: &span, "sync session finished");

        Ok(())
    }
}

impl RhioSyncProtocol {
    pub fn new(config: Config, nats: Nats, blob_store: S3Store, private_key: PrivateKey) -> Self {
        Self {
            config,
            nats,
            blob_store,
            private_key,
        }
    }

    /// Get a list of blobs we have ourselves already in the blob store for this query.
    async fn complete_blobs(
        &self,
        bucket_name: &BucketName,
    ) -> Vec<(BlobHash, BucketName, Paths, ObjectSize)> {
        self.blob_store
            .complete_blobs()
            .await
            .into_iter()
            .filter(|(_, store_bucket_name, _, _)| store_bucket_name == bucket_name)
            .collect()
    }

    /// Download all NATS messages we have for that subject and return them as a stream.
    async fn nats_stream(
        &self,
        stream_name: String,
        subject: &Subject,
        topic_id: [u8; 32],
    ) -> Result<(ConsumerId, BoxStream<Result<NatsMessage, SyncError>>), SyncError> {
        let (consumer_id, nats_rx) = self
            .nats
            .subscribe(
                stream_name,
                subject.to_owned(),
                DeliverPolicy::All,
                topic_id,
            )
            .await
            .map_err(|err| {
                SyncError::Critical(format!("can't subscribe to NATS stream: {}", err))
            })?;

        let nats_stream = BroadcastStream::new(nats_rx)
            .take_while(|event| {
                // Take messages from stream until we've reached all currently known messages, do
                // not wait for upcoming, future messages.
                future::ready(!matches!(event, Ok(JetStreamEvent::InitCompleted { .. })))
            })
            .filter_map(|message| async {
                match message {
                    Ok(JetStreamEvent::Message { message, .. }) => Some(Ok(message)),
                    Ok(JetStreamEvent::Failed { reason, .. }) => Some(Err(SyncError::Critical(
                        format!("could not download all past messages from nats server: {reason}"),
                    ))),
                    Err(err) => Some(Err(SyncError::Critical(format!(
                        "broadcast stream failed: {err}"
                    )))),
                    Ok(JetStreamEvent::InitCompleted { .. }) => {
                        unreachable!("init complete events got filtered out before")
                    }
                }
            });

        Ok((consumer_id, Box::pin(nats_stream)))
    }

    async fn unsubscribe_nats_stream(&self, consumer_id: ConsumerId) -> Result<(), SyncError> {
        self.nats.unsubscribe(consumer_id).await.map_err(|err| {
            SyncError::Critical(format!("can't unsubscribe from NATS stream: {}", err))
        })?;
        Ok(())
    }
}

fn init_handshake(query: Query) -> (Message, Challenge) {
    let challenge: u64 = random();
    (Message::Handshake(query, challenge), challenge)
}

fn accept_handshake(challenge: Challenge, private_key: &PrivateKey) -> Message {
    let signature = private_key.sign(&challenge.to_ne_bytes());
    Message::HandshakeResponse(signature)
}

fn verify_handshake(signature: &Signature, challenge: Challenge, public_key: &PublicKey) -> bool {
    public_key.verify(&challenge.to_ne_bytes(), signature)
}

#[cfg(test)]
mod tests {
    use p2panda_core::PrivateKey;

    use crate::topic::Query;

    use super::{accept_handshake, init_handshake, verify_handshake, Message};

    #[test]
    fn handshake() {
        let subscribe_private_key = PrivateKey::new();
        let subscribe_public_key = subscribe_private_key.public_key();
        let query = Query::Bucket {
            bucket_name: "bucket-1".into(),
            // We're interested in the data of this peer.
            public_key: subscribe_public_key,
        };

        // 1. Initializing peer sends handshake to remote and keeps challenge around.
        let (init_message, challenge) = init_handshake(query.clone());
        assert!(matches!(init_message, Message::Handshake(..)));

        // Challenges should be different each time.
        let (_, challenge_again) = init_handshake(query);
        assert_ne!(challenge, challenge_again);

        // 2. Accepting peer signs challenge with their own private key and sends it back.
        let accept_message = accept_handshake(challenge, &subscribe_private_key);

        // 3. Initializing peer verifies signature with challenge.
        let Message::HandshakeResponse(signature) = accept_message else {
            panic!("this is not a handshake response");
        };
        assert!(verify_handshake(
            &signature,
            challenge,
            &subscribe_public_key
        ))
    }
}
