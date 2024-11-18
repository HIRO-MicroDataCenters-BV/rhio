use std::collections::HashSet;
use std::sync::Arc;

use async_nats::jetstream::consumer::DeliverPolicy;
use async_nats::message::Message as NatsMessage;
use async_trait::async_trait;
use futures_util::future::{self};
use futures_util::stream::BoxStream;
use futures_util::{pin_mut, AsyncRead, AsyncWrite, Sink, SinkExt, StreamExt};
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
use crate::nats::message::hash_nats_message;
use crate::nats::{ConsumerId, JetStreamEvent, Nats};
use crate::topic::Query;

/// Simple sync protocol implementation to allow exchange of past NATS messages or blob
/// announcements.
///
/// The protocol is roughly as follows:
///
/// ```text
///                    I. HANDSHAKE PHASE
/// [Initiator]                                  [Acceptor]
///
/// 1.           ------> Send Handshake ------->
/// 2. Exit when No-Sync                         Exit when No-Sync
/// 3.                                           Solve Challenge
/// 4.           <----- Respond Handshake <-----
/// 5. Exit when invalid peer                    Exit when invalid peer
///
///                       II. SYNC PHASE
/// [Initiator]                                  [Acceptor]
///
/// 6.           Send hashes of data we have -->
/// 7.           -------> Send Done message --->
/// 8.                                           Exit or calculate delta
/// 9.           <------ Send delta data we have
/// 10.          <------- Send Done message <---
/// 11. Ingest
/// ```
// @TODO(adz): This implementation is sub-optimal in multiple ways:
// - It requires the peers to send over _everything_ they know about. This can be optimized later
// with a smarter set reconciliation strategy though it'll be tricky to find out how to organize
// the data to make it more efficient (NATS messages do not have timestamps but we could sort them
// by sequential order of the filtered consumer, blobs could be sorted by S3 key (the absolute
// path)?).
// - "No Sync" workaround (see comment below).
// - Acceptor can exit _after_ all data from initiator has been sent already. This can happen when
// the acceptor realizes that it doesn't publish the requested data.
#[derive(Clone, Debug)]
pub struct RhioSyncProtocol {
    config: Config,
    nats: Nats,
    blob_store: S3Store,
    private_key: PrivateKey,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "value")]
pub enum Message {
    #[serde(rename = "handshake")]
    Handshake(Query, u64),

    #[serde(rename = "handshake_response")]
    HandshakeResponse(Signature),

    #[serde(rename = "nats_have")]
    NatsHave(Hash),

    #[serde(rename = "nats_have_done")]
    NatsHaveDone,

    #[serde(rename = "nats_data")]
    NatsData(NetworkMessage),

    #[serde(rename = "nats_done")]
    NatsDone,

    #[serde(rename = "blobs_have")]
    BlobsHave(BlobHash),

    #[serde(rename = "blobs_have_done")]
    BlobsHaveDone,

    #[serde(rename = "blobs_data")]
    BlobsData(NetworkMessage),

    #[serde(rename = "blobs_done")]
    BlobsDone,
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

        // I. HANDSHAKE PHASE
        // ~~~~~~~~~~~~~~~~~~

        // Inform p2panda backend about query.
        app_tx
            .send(FromSync::HandshakeSuccess(query.clone()))
            .await?;

        // 1. Send handshake message over to other peer.
        //
        // - So they'll learn what we want from them via our "query".
        // - They'll receive the nonce for the handshake challenge.
        debug!(parent: &span, "sending sync query {query}");
        let (message, nonce) = init_handshake(query.clone());
        sink.send(message).await?;

        // 2. End prematurely when we don't want to sync.
        //
        // @TODO(adz): This is a workaround to disable syncing in some cases, for example when
        // we're a publishing peer we don't want to initiate syncing.
        //
        // The current p2panda API does not give any control to turn off syncing for some data
        // stream subscriptions, this is why we're doing it this hacky way.
        if query.is_no_sync() {
            debug!(parent: &span, "end sync session prematurely as we don't want to have one");
            return Ok(());
        }

        // 5. Verify that this peer is the one we're subscribed to by checking it's signature.
        let message = stream.next().await.ok_or(SyncError::UnexpectedBehaviour(
            "incoming message stream ended prematurely".into(),
        ))??;
        let Message::HandshakeResponse(signature) = message else {
            return Err(SyncError::UnexpectedBehaviour(
                "did not receive expected message".into(),
            ));
        };

        if !verify_handshake(&signature, nonce, query.public_key()) {
            debug!(parent: &span, "end sync session as remote peer did not match our subscribed public keys");
            return Ok(());
        }

        // II. SYNC PHASE
        // ~~~~~~~~~~~~~~

        // We can sync over NATS messages or S3 blob announcements.
        match query {
            Query::Bucket {
                ref bucket_name, ..
            } => {
                // 6. Send over a list of blob hashes we have already to remote peer.
                let blob_hashes: Vec<BlobHash> = self
                    .complete_blobs(bucket_name)
                    .await
                    .iter()
                    .map(|(hash, _, _, _)| hash.to_owned())
                    .collect();
                debug!(parent: &span, "we have {} completed blobs", blob_hashes.len());

                for blob_hash in blob_hashes {
                    sink.send(Message::BlobsHave(blob_hash)).await?;
                }

                // 7. Finalize sending what we have.
                sink.send(Message::BlobsHaveDone).await?;

                // 11. Wait for other peer to send us what we're missing.
                let mut counter = 0;
                loop {
                    let message = stream.next().await.ok_or(SyncError::UnexpectedBehaviour(
                        "incoming message stream ended prematurely".into(),
                    ))??;

                    match message {
                        Message::BlobsData(blob_announcement) => {
                            counter += 1;

                            // Send data to p2panda backend.
                            app_tx
                                .send(FromSync::Data(blob_announcement.to_bytes(), None))
                                .await?;
                        }
                        Message::BlobsDone => {
                            break;
                        }
                        _ => {
                            return Err(SyncError::UnexpectedBehaviour(
                                "did not receive expected message".into(),
                            ));
                        }
                    }
                }

                debug!(parent: &span,
                    "received {} new blob announcements from remote peer",
                    counter,
                );
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

                // 6. Download all NATS messages we have from the server for this subject and hash
                //    them each. We send all hashes over to the other peer so they can determine
                //    and send us what we don't have.
                let (consumer_id, nats_stream) =
                    self.nats_stream(stream_name, subject, query.id()).await?;
                let mut nats_stream = nats_stream.map(|event| match event {
                    Ok(message) => Ok(hash_nats_message(&message)),
                    Err(err) => Err(err),
                });

                let mut counter = 0;
                while let Some(nats_hash) = nats_stream.next().await {
                    match nats_hash {
                        Ok(nats_hash) => {
                            counter += 1;
                            sink.send(Message::NatsHave(nats_hash)).await?;
                        }
                        Err(err) => {
                            // Do not forget to unsubscribe consumer even on failure.
                            self.unsubscribe_nats_stream(consumer_id).await?;
                            return Err(err);
                        }
                    }
                }

                self.unsubscribe_nats_stream(consumer_id).await?;

                // 7. Finalize sending what we have.
                sink.send(Message::NatsHaveDone).await?;

                debug!(parent: &span,
                    "downloaded {} NATS messages",
                    counter,
                );

                // 11. Wait for other peer to send us what we're missing.
                let mut counter = 0;
                loop {
                    let message = stream.next().await.ok_or(SyncError::UnexpectedBehaviour(
                        "incoming message stream ended prematurely".into(),
                    ))??;

                    match message {
                        Message::NatsData(nats_message) => {
                            counter += 1;
                            app_tx
                                .send(FromSync::Data(nats_message.to_bytes(), None))
                                .await?;
                        }
                        Message::NatsDone => {
                            break;
                        }
                        _ => {
                            return Err(SyncError::UnexpectedBehaviour(
                                "did not receive expected message".into(),
                            ));
                        }
                    }
                }

                debug!(parent: &span,
                    "received {} new NATS messages from remote peer",
                    counter,
                );
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

        // I. HANDSHAKE PHASE
        // ~~~~~~~~~~~~~~~~~~

        // 1. Expect initiating peer to tell us what they want to sync.
        let message = stream.next().await.ok_or(SyncError::UnexpectedBehaviour(
            "incoming message stream ended prematurely".into(),
        ))??;
        let Message::Handshake(query, nonce) = message else {
            return Err(SyncError::UnexpectedBehaviour(
                "did not receive expected message".into(),
            ));
        };

        let span = span!(Level::DEBUG, "acceptor", query = %query);
        debug!(parent: &span, "received sync query {}", query);

        // Tell p2panda backend about query.
        app_tx
            .send(FromSync::HandshakeSuccess(query.clone()))
            .await?;

        // 2. The other peer might tell us sometimes that they _don't_ want to sync.
        //
        // @TODO(adz): This is a workaround to disable syncing in some cases as the current p2panda
        // API does not give any control to turn off syncing for some topics.
        if query.is_no_sync() {
            debug!(parent: &span, "end sync session prematurely as we don't want to have one");
            return Ok(());
        }

        // 3. & 4. & 5. Check if we are the peer the initiator is asking for, proof it if we are,
        //    otherwise send invalid proof and stop here.
        let accept_message = accept_handshake(nonce, &self.private_key);
        sink.send(accept_message).await?;

        if query.public_key() != &self.private_key.public_key() {
            debug!(parent: &span, "end sync session prematurely as we are not the peer the initiator asked for");
            return Ok(());
        }

        // II. SYNC PHASE
        // ~~~~~~~~~~~~~~

        // We can sync over NATS messages or S3 blob announcements.
        match &query {
            Query::Bucket { bucket_name, .. } => {
                // 8. Await message from other peer on the blobs they _have_, so we can calculate
                //    what they're missing and send that delta to them.
                let mut remote_blob_hashes = HashSet::new();
                loop {
                    let message = stream.next().await.ok_or(SyncError::UnexpectedBehaviour(
                        "incoming message stream ended prematurely".into(),
                    ))??;

                    match message {
                        Message::BlobsHave(blob_hash) => {
                            remote_blob_hashes.insert(blob_hash);
                        }
                        Message::BlobsHaveDone => {
                            break;
                        }
                        _ => {
                            return Err(SyncError::UnexpectedBehaviour(
                                "did not receive expected message".into(),
                            ));
                        }
                    }
                }

                debug!(parent: &span,
                    "received {} hashes from remote peer",
                    remote_blob_hashes.len()
                );

                let is_publishing = match &self.config.publish {
                    Some(publications) => publications
                        .s3_buckets
                        .iter()
                        .find(|local_bucket_name| &bucket_name == local_bucket_name),
                    None => None,
                }
                .is_some();

                if !is_publishing {
                    // Inform the other peer politely that we need to end here as we can't provide
                    // for this S3 bucket.
                    debug!(parent: &span, "can't provide data, politely end sync");
                    sink.send(Message::BlobsDone).await?;
                    return Ok(());
                }

                // 9. Send back delta data to other peer.
                let mut counter = 0;
                for (hash, _, paths, size) in self.complete_blobs(bucket_name).await {
                    if !remote_blob_hashes.contains(&hash) {
                        let blob_announcement = {
                            let mut signed_msg = NetworkMessage::new_blob_announcement(
                                hash,
                                bucket_name.clone(),
                                paths.data(),
                                size,
                            );
                            signed_msg.sign(&self.private_key);
                            signed_msg
                        };
                        counter += 1;
                        sink.send(Message::BlobsData(blob_announcement)).await?;
                    }
                }

                // 10. Finalize sync session.
                sink.send(Message::BlobsDone).await?;

                debug!(parent: &span, "send {} blob announcements", counter);
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

                // 8. Await message from other peer on the NATS messages they _have_, so we can
                //    calculate what they're missing and send that delta to them.
                let mut remote_nats_hashes = HashSet::new();
                loop {
                    let message = stream.next().await.ok_or(SyncError::UnexpectedBehaviour(
                        "incoming message stream ended prematurely".into(),
                    ))??;

                    match message {
                        Message::NatsHave(nats_hash) => {
                            remote_nats_hashes.insert(nats_hash);
                        }
                        Message::NatsHaveDone => {
                            break;
                        }
                        _ => {
                            return Err(SyncError::UnexpectedBehaviour(
                                "did not receive expected message".into(),
                            ));
                        }
                    }
                }

                debug!(parent: &span,
                    "received {} hashes from remote peer",
                    remote_nats_hashes.len()
                );

                // Inform the other peer politely that we need to end here as we can't provide for
                // this NATS subject.
                let Some(stream_name) = stream_name else {
                    debug!(parent: &span, "can't provide data, politely end sync");
                    sink.send(Message::NatsDone).await?;
                    return Ok(());
                };

                // 9. Send back delta data to other peer.
                let (consumer_id, nats_stream) =
                    self.nats_stream(stream_name, subject, query.id()).await?;
                let nats_stream = nats_stream.filter_map(|event| async {
                    match event {
                        Ok(message) => {
                            let hash = hash_nats_message(&message);
                            if remote_nats_hashes.contains(&hash) {
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

                pin_mut!(nats_stream);

                let mut counter = 0;
                while let Some(nats_message) = nats_stream.next().await {
                    match nats_message {
                        Ok(nats_message) => {
                            counter += 1;
                            sink.send(Message::NatsData(nats_message)).await?;
                        }
                        Err(err) => {
                            self.unsubscribe_nats_stream(consumer_id).await?;
                            return Err(err);
                        }
                    }
                }

                self.unsubscribe_nats_stream(consumer_id).await?;

                // 10. Finalize sync session.
                sink.send(Message::NatsDone).await?;

                debug!(parent: &span, "downloaded {} NATS messages", counter);
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

fn init_handshake(query: Query) -> (Message, u64) {
    let nonce: u64 = random();
    (Message::Handshake(query, nonce), nonce)
}

fn accept_handshake(nonce: u64, private_key: &PrivateKey) -> Message {
    let signature = private_key.sign(&nonce.to_ne_bytes());
    Message::HandshakeResponse(signature)
}

fn verify_handshake(signature: &Signature, nonce: u64, public_key: &PublicKey) -> bool {
    public_key.verify(&nonce.to_ne_bytes(), signature)
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

        // 1. Initializing peer sends handshake to remote and keeps nonce around.
        let (init_message, nonce) = init_handshake(query.clone());
        assert!(matches!(init_message, Message::Handshake(..)));

        // Nonce should be different each time.
        let (_, nonce_again) = init_handshake(query);
        assert_ne!(nonce, nonce_again);

        // 2. Accepting peer signs nonce with their own private key and sends it back.
        let accept_message = accept_handshake(nonce, &subscribe_private_key);

        // 3. Initializing peer verifies signature with nonce.
        let Message::HandshakeResponse(signature) = accept_message else {
            panic!("this is not a handshake response");
        };
        assert!(verify_handshake(&signature, nonce, &subscribe_public_key))
    }
}
