use std::sync::Arc;

use async_nats::jetstream::consumer::DeliverPolicy;
use async_nats::message::Message as NatsMessage;
use async_trait::async_trait;
use futures_util::future::{self};
use futures_util::stream::BoxStream;
use futures_util::{AsyncRead, AsyncWrite, Sink, SinkExt, StreamExt, TryStreamExt};
use p2panda_core::{Hash, PrivateKey};
use p2panda_net::TopicId;
use p2panda_sync::cbor::{into_cbor_sink, into_cbor_stream};
use p2panda_sync::{FromSync, SyncError, SyncProtocol};
use rhio_core::{NetworkMessage, ScopedSubject};
use serde::{Deserialize, Serialize};
use tokio_stream::wrappers::BroadcastStream;
use tracing::{debug, span, Level};

use crate::config::Config;
use crate::nats::{JetStreamEvent, Nats};
use crate::topic::Query;

static SYNC_PROTOCOL_NAME: &str = "rhio-sync-v1";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Message {
    Query(Query),
    NatsHave(Vec<Hash>),
    NatsMessages(Vec<NetworkMessage>),
}

#[derive(Clone, Debug)]
pub struct RhioSyncProtocol {
    config: Config,
    nats: Nats,
    private_key: PrivateKey,
}

impl RhioSyncProtocol {
    pub fn new(config: Config, nats: Nats, private_key: PrivateKey) -> Self {
        Self {
            config,
            nats,
            private_key,
        }
    }

    /// Download all NATS messages we have for that subject and return them as a stream.
    async fn nats_stream(
        &self,
        stream_name: String,
        subject: &ScopedSubject,
        topic_id: [u8; 32],
    ) -> Result<BoxStream<Result<NatsMessage, SyncError>>, SyncError> {
        let nats_rx = self
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
                    Ok(JetStreamEvent::InitFailed { reason, .. }) => {
                        Some(Err(SyncError::Critical(format!(
                            "could not download all past messages from nats server: {reason}"
                        ))))
                    }
                    Ok(JetStreamEvent::StreamFailed { reason, .. }) => {
                        Some(Err(SyncError::Critical(format!(
                            "could not download all past messages from nats server: {reason}"
                        ))))
                    }
                    Err(err) => Some(Err(SyncError::Critical(format!(
                        "broadcast stream failed: {err}"
                    )))),
                    Ok(JetStreamEvent::InitCompleted { .. }) => {
                        unreachable!("init complete events got filtered out before")
                    }
                }
            });

        Ok(Box::pin(nats_stream))
    }
}

#[async_trait]
impl<'a> SyncProtocol<'a, Query> for RhioSyncProtocol {
    fn name(&self) -> &'static str {
        SYNC_PROTOCOL_NAME
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
        sink.send(Message::Query(query.clone())).await?;

        // @TODO(adz): This is a workaround to disable syncing in some cases, for example when
        // we're a publishing peer we don't want to initiate syncing.
        //
        // The current p2panda API does not give any control to turn off syncing for some data
        // stream subscriptions, this is why we're doing it this hacky way.
        if matches!(query, Query::NoSyncSubject { .. }) {
            debug!(parent: &span, "end sync session prematurely as we don't want to have one");
            return Ok(());
        }

        // 2. We can sync over NATS messages or S3 blob announcements.
        match query {
            Query::Bucket { .. } => todo!(),
            Query::Subject { ref subject } => {
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
                let nats_stream = self.nats_stream(stream_name, subject, query.id()).await?;
                let nats_stream = nats_stream.map(|event| match event {
                    Ok(message) => Ok(Hash::new(&message.payload)),
                    Err(err) => Err(err),
                });
                let nats_message_hashes: Vec<Hash> = nats_stream.try_collect().await?;
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
        let Message::Query(query) = message else {
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
        if matches!(query, Query::NoSyncSubject { .. }) {
            debug!(parent: &span, "end sync session prematurely as we don't want to have one");
            return Ok(());
        }

        // 2. We can sync over NATS messages or S3 blob announcements.
        match &query {
            Query::Bucket { .. } => todo!(),
            Query::Subject { subject } => {
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

                let nats_stream = self.nats_stream(stream_name, subject, query.id()).await?;
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
                let nats_messages: Vec<NetworkMessage> = nats_stream.try_collect().await?;
                debug!(parent: &span, "downloaded {} NATS messages", nats_messages.len());

                sink.send(Message::NatsMessages(nats_messages)).await?;
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
