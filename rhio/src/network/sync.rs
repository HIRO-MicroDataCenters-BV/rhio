use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use async_nats::jetstream::consumer::DeliverPolicy;
use async_nats::message::Message as NatsMessage;
use async_trait::async_trait;
use futures_util::future::{self};
use futures_util::stream::BoxStream;
use futures_util::{pin_mut, AsyncRead, AsyncWrite, Sink, SinkExt, StreamExt};
use p2panda_core::{Hash, PrivateKey, PublicKey};
use p2panda_net::TopicId;
use p2panda_sync::cbor::{into_cbor_sink, into_cbor_stream};
use p2panda_sync::{FromSync, SyncError, SyncProtocol};
use rand::random;
use rhio_blobs::{BlobHash, CompletedBlob, S3Store};
use rhio_core::{nats, NetworkMessage, Subject};
use serde::{Deserialize, Serialize};
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamMap;
use tracing::{debug, span, warn, Level};

use crate::nats::{ConsumerId, JetStreamEvent, Nats, StreamName};
use crate::node::config::NodeConfig;
use crate::topic::Query;
use crate::FilteredMessageStream;

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
///
///                       II. SYNC PHASE
/// [Initiator]                                  [Acceptor]
///
/// 3.           Send hashes of data we have -->
/// 4.           -------> Send Done message --->
/// 5.                                           Exit or calculate delta
/// 6.           <------ Send delta data we have
/// 7.           <------- Send Done message <---
///
/// Ingest!
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
    config: NodeConfig,
    nats: Nats,
    blob_store: S3Store,
    private_key: PrivateKey,
    public_key: PublicKey,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "value")]
pub enum Message {
    #[serde(rename = "handshake")]
    Handshake(Query),

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
        let session_id: [u8; 2] = random();
        let span = span!(Level::DEBUG, "initiator", session_id = hex::encode(session_id), %query);

        let mut sink = into_cbor_sink(tx);
        let mut stream = into_cbor_stream(rx);

        // I. HANDSHAKE PHASE
        // ~~~~~~~~~~~~~~~~~~

        // Inform p2panda backend about query.
        app_tx
            .send(FromSync::HandshakeSuccess(query.clone()))
            .await?;

        // 1. Send handshake message over to other peer so that remote peer learns what we would
        //    like to sync.
        debug!(parent: &span, "sending sync query {query}");
        sink.send(Message::Handshake(query.clone())).await?;

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

        // II. SYNC PHASE
        // ~~~~~~~~~~~~~~

        // We can sync over NATS messages or blob announcements.
        match query {
            Query::Files {
                public_key: requested_public_key,
            } => {
                assert!(
                    requested_public_key != self.public_key,
                    "we never initiate sync sessions for our own data"
                );

                // 3. Send over a list of blob hashes we have already to remote peer.
                let blob_hashes: Vec<BlobHash> = self
                    .complete_blobs()
                    .await
                    .iter()
                    .filter_map(|blob| {
                        // We're only interested in remote peer's blobs when initiating a sync
                        // session, so skip the unsigned ones (which are our blobs).
                        let CompletedBlob::Signed(blob) = blob else {
                            return None;
                        };

                        // Filter out all blobs which are not from that peer.
                        if blob.public_key != requested_public_key {
                            return None;
                        }

                        Some(blob.hash)
                    })
                    .collect();
                debug!(parent: &span, "we have {} completed blobs", blob_hashes.len());

                for blob_hash in blob_hashes {
                    sink.send(Message::BlobsHave(blob_hash)).await?;
                }

                // 4. Finalize sending what we have.
                sink.send(Message::BlobsHaveDone).await?;

                // Wait for other peer to send us what we're missing and ingest it!
                let mut counter = 0;
                loop {
                    let message = stream.next().await.ok_or(SyncError::UnexpectedBehaviour(
                        "incoming message stream ended prematurely".into(),
                    ))??;

                    match message {
                        Message::BlobsData(blob_announcement) => {
                            counter += 1;

                            // "Ingest" data to p2panda backend.
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
            Query::Messages {
                subjects: ref requested_subjects,
                public_key: requested_public_key,
                ..
            } => {
                assert!(
                    requested_public_key != self.public_key,
                    "we never initiate sync sessions for our own data"
                );

                // We have a list of all NATS subjects we are interested in for that public key.
                // Each subject can be handled by a different NATS stream (based on our local
                // configuration) and we need to first find out which ones they are.
                let streams = self
                    .streams_for_subjects(requested_subjects, &requested_public_key)
                    .await;
                assert!(!streams.is_empty(), "query matches config");

                // 3. Download all NATS messages we have from the NATS server for this subject and
                //    hash them each. We send all hashes over to the other peer so they can
                //    determine and send us what we don't have.
                let (consumer_ids, nats_stream) =
                    self.muxed_nats_streams(streams, query.id()).await?;
                let nats_stream = nats_stream.filter_map(|(stream_info, event)| async {
                    match event {
                        Ok(message) => {
                            // Remove all messages which are not from the public key we are
                            // interested in.
                            if !nats::matching_public_key(&message, &requested_public_key) {
                                return None;
                            }

                            match nats::wrap_and_sign_nats_message(message, &self.private_key) {
                                Ok(network_message) => Some((stream_info, Ok(network_message.hash()))),
                                Err(err) => {
                                    warn!("detected invalid signature of NATS message in stream: {err}");
                                    None
                                },
                            }
                        }
                        Err(err) => Some((stream_info, Err(err))),
                    }
                });
                pin_mut!(nats_stream);

                let mut counter = 0;
                while let Some((stream_info, nats_hash)) = nats_stream.next().await {
                    match nats_hash {
                        Ok(nats_hash) => {
                            counter += 1;
                            sink.send(Message::NatsHave(nats_hash)).await?;
                        }
                        Err(err) => {
                            // Do not forget to unsubscribe consumer on failure.
                            let consumer_id = stream_info.1;
                            self.unsubscribe_nats_stream(consumer_id).await?;
                            return Err(err);
                        }
                    }
                }

                // Do not forget to unsubscribe from all consumers on success.
                for consumer_id in consumer_ids {
                    self.unsubscribe_nats_stream(consumer_id).await?;
                }

                // 4. Finalize sending what we have.
                sink.send(Message::NatsHaveDone).await?;

                debug!(parent: &span,
                    "downloaded {} NATS messages",
                    counter,
                );

                //  Wait for other peer to send us what we're missing and ingest it!
                let mut counter = 0;
                loop {
                    let message = stream.next().await.ok_or(SyncError::UnexpectedBehaviour(
                        "incoming message stream ended prematurely".into(),
                    ))??;

                    match message {
                        Message::NatsData(nats_message) => {
                            counter += 1;

                            // "Ingest" data to p2panda backend.
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
            Query::NoSyncFiles { .. } => {
                unreachable!("returned already earlier on no-sync option")
            }
            Query::NoSyncMessages { .. } => {
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
        let Message::Handshake(query) = message else {
            return Err(SyncError::UnexpectedBehaviour(
                "did not receive expected message".into(),
            ));
        };

        let session_id: [u8; 2] = random();
        let span = span!(Level::DEBUG, "acceptor", session_id = hex::encode(session_id),  %query);
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

        // II. SYNC PHASE
        // ~~~~~~~~~~~~~~

        // We can sync over NATS messages or blob announcements.
        match &query {
            Query::Files {
                public_key: requested_public_key,
            } => {
                // 5. Await message from other peer on the blobs they _have_, so we can calculate
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

                // We're not only publishing data but also "forward" blobs from other peers.
                let is_forwarding = self
                    .config
                    .is_files_subscription_matching(requested_public_key)
                    .await
                    .is_some();

                // Can we forward someone elses files or ours?
                if !is_forwarding && requested_public_key != &self.public_key {
                    // Inform the other peer politely that we need to end here as we can't provide
                    // data from this public key.
                    debug!(parent: &span, "can't provide data, politely end sync");
                    sink.send(Message::BlobsDone).await?;
                    return Ok(());
                }

                // 6. Send back delta data to other peer.
                let mut counter = 0;
                for blob in self.complete_blobs().await {
                    match blob {
                        CompletedBlob::Unsigned(_) => {
                            // Remote peer did not ask for our data.
                            if requested_public_key != &self.public_key {
                                continue;
                            }
                        }
                        CompletedBlob::Signed(ref blob) => {
                            // Remote peer did not ask for this peer's data.
                            if requested_public_key != &blob.public_key {
                                continue;
                            }
                        }
                    }

                    if !remote_blob_hashes.contains(&blob.hash()) {
                        let blob_announcement = {
                            match &blob {
                                // Sign our own blobs before sending them over.
                                CompletedBlob::Unsigned(blob) => {
                                    let mut signed_msg = NetworkMessage::new_blob_announcement(
                                        blob.hash,
                                        blob.key.clone(),
                                        blob.size,
                                        &self.public_key,
                                    );
                                    signed_msg.sign(&self.private_key);
                                    signed_msg
                                }
                                // Just forward already-signed blobs if this what the remote peer
                                // asked for.
                                CompletedBlob::Signed(blob) => {
                                    NetworkMessage::new_signed_blob_announcement(blob.clone())
                                }
                            }
                        };
                        counter += 1;
                        sink.send(Message::BlobsData(blob_announcement)).await?;
                    }
                }

                // 7. Finalize sync session.
                sink.send(Message::BlobsDone).await?;

                debug!(parent: &span, "send {} blob announcements", counter);
            }
            Query::Messages {
                subjects: requested_subjects,
                public_key: requested_public_key,
                ..
            } => {
                // Look up our config to find out if we have a NATS stream somewhere which fits the
                // requested subject and public key.
                //
                // - Are we publishing the requested data ourselves?
                let our_streams = {
                    let mut result = Vec::new();
                    if requested_public_key == &self.public_key {
                        let publications = self.config.message_publications().await;
                        for stream in &publications {
                            for subject in requested_subjects {
                                if !stream.subjects.contains(subject) {
                                    continue;
                                }
                                result.push(stream.clone());
                            }
                        }
                    }
                    result
                };
                // - Can we forward data from someone else?
                let forwarded_streams = self
                    .streams_for_subjects(requested_subjects, requested_public_key)
                    .await;

                // 5. Await message from other peer on the NATS messages they _have_, so we can
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
                // this NATS subject or public key.
                if our_streams.is_empty() && forwarded_streams.is_empty() {
                    debug!(parent: &span, "can't provide data, politely end sync");
                    sink.send(Message::NatsDone).await?;
                    return Ok(());
                };

                // 6. Send back delta data to other peer.
                let (consumer_ids, nats_stream) = {
                    if !our_streams.is_empty() {
                        self.muxed_nats_streams(our_streams, query.id()).await?
                    } else {
                        self.muxed_nats_streams(forwarded_streams, query.id())
                            .await?
                    }
                };

                let nats_stream = nats_stream.filter_map(|(stream_info, event)| async {
                    match event {
                        Ok(message) => {
                            // Remove all messages which are not from the public key we are
                            // interested in.
                            //
                            // If no public key is given in the NATS message header, we assume it's
                            // our message and check if that's what the remote peer was interested
                            // in.
                            if !nats::matching_public_key(&message, requested_public_key)
                                && !nats::matching_public_key(&message, &self.public_key)
                                    && nats::has_nats_signature(&message.headers)
                            {
                                return None;
                            }

                            let network_message = match nats::wrap_and_sign_nats_message(message, &self.private_key) {
                                Ok(network_message) => network_message,
                                Err(err) => {
                                    // Filter out invalid NATS signatures (they should have not
                                    // arrived here at this point though).
                                    warn!("detected invalid signature of NATS message in stream: {err}");
                                    return None;
                                },
                            };

                            if remote_nats_hashes.contains(&network_message.hash()) {
                                None
                            } else {
                                Some((stream_info, Ok(network_message)))
                            }
                        }
                        Err(err) => Some((stream_info, Err(err))),
                    }
                });

                pin_mut!(nats_stream);

                let mut counter = 0;
                while let Some((stream_info, nats_message)) = nats_stream.next().await {
                    match nats_message {
                        Ok(nats_message) => {
                            counter += 1;
                            sink.send(Message::NatsData(nats_message)).await?;
                        }
                        Err(err) => {
                            // Do not forget to unsubscribe consumer on failure.
                            let consumer_id = stream_info.1;
                            self.unsubscribe_nats_stream(consumer_id).await?;
                            return Err(err);
                        }
                    }
                }

                // Do not forget to unsubscribe from all consumers on success.
                for consumer_id in consumer_ids {
                    self.unsubscribe_nats_stream(consumer_id).await?;
                }

                // 7. Finalize sync session.
                sink.send(Message::NatsDone).await?;

                debug!(parent: &span, "downloaded {} NATS messages", counter);
            }
            Query::NoSyncFiles { .. } => {
                unreachable!("we've already returned before no-sync option")
            }
            Query::NoSyncMessages { .. } => {
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
    pub fn new(
        config: NodeConfig,
        nats: Nats,
        blob_store: S3Store,
        private_key: PrivateKey,
    ) -> Self {
        Self {
            config,
            nats,
            blob_store,
            public_key: private_key.public_key(),
            private_key,
        }
    }

    /// Get a list of blobs we have ourselves in the blob store.
    async fn complete_blobs(&self) -> Vec<CompletedBlob> {
        self.blob_store.complete_blobs().await.into_iter().collect()
    }

    /// Finds out which subjects for this public key can be hooked into which NATS stream based on
    /// our configuration.
    ///
    /// Returns a list of the NATS streams and the regarding (de-duplicated) subjects we can use on
    /// top of them or an empty array if there's nothing matching.
    async fn streams_for_subjects(
        &self,
        subjects: &Vec<Subject>,
        public_key: &PublicKey,
    ) -> Vec<FilteredMessageStream> {
        let mut visited_subjects = HashSet::<Subject>::new();
        let mut stream_map = HashMap::<StreamName, Vec<Subject>>::new();

        let subscriptions = self.config.message_subscriptions().await;
        for subscription in &subscriptions {
            // Filter by public key.
            if &subscription.public_key != public_key {
                continue;
            }

            for stream in &subscription.filtered_streams {
                for subject in subjects {
                    // Filter by subject.
                    if !stream.subjects.contains(subject) {
                        continue;
                    }

                    // De-Duplicate subjects, we only need one stream for each.
                    let is_new = visited_subjects.insert(subject.clone());
                    if !is_new {
                        continue;
                    }

                    stream_map
                        .entry(stream.stream_name.clone())
                        .and_modify(|subjects| subjects.push(subject.clone()))
                        .or_insert(vec![subject.clone()]);
                }
            }
        }

        stream_map
            .into_iter()
            .map(|(stream_name, subjects)| FilteredMessageStream {
                subjects,
                stream_name,
            })
            .collect()
    }

    /// Download all NATS messages we have for these subjects and return them as a stream.
    async fn muxed_nats_streams(
        &self,
        stream_configs: Vec<FilteredMessageStream>,
        topic_id: [u8; 32],
    ) -> Result<
        (
            Vec<ConsumerId>,
            StreamMap<(StreamName, ConsumerId), BoxStream<Result<NatsMessage, SyncError>>>,
        ),
        SyncError,
    > {
        let mut consumer_ids = Vec::new();
        let mut map = StreamMap::new();
        for config in stream_configs {
            let (consumer_id, stream) = self
                .nats_stream(config.stream_name.clone(), config.subjects, topic_id)
                .await?;
            consumer_ids.push(consumer_id.clone());
            map.insert((config.stream_name, consumer_id), stream);
        }
        Ok((consumer_ids, map))
    }

    async fn nats_stream(
        &self,
        stream_name: StreamName,
        subjects: Vec<Subject>,
        topic_id: [u8; 32],
    ) -> Result<(ConsumerId, BoxStream<Result<NatsMessage, SyncError>>), SyncError> {
        let (consumer_id, nats_rx) = self
            .nats
            .subscribe(stream_name, subjects, DeliverPolicy::All, topic_id)
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
