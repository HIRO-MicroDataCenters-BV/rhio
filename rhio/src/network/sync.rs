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
use rhio_core::NetworkMessage;
use serde::{Deserialize, Serialize};
use tokio_stream::wrappers::BroadcastStream;
use tracing::debug;

use crate::config::Config;
use crate::nats::{JetStreamEvent, Nats};
use crate::topic::Query;

static SYNC_PROTOCOL_NAME: &str = "rhio-sync-v1";
static NATS_FROM_RHIO_HEADER: &str = "X-Rhio-From-Sync";

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

    async fn nats_stream(
        &self,
        stream_name: String,
        query: Query,
    ) -> Result<BoxStream<Result<NatsMessage, SyncError>>, SyncError> {
        let nats_rx = match query {
            Query::Bucket { bucket_name: _ } => todo!(),
            Query::Subject { ref subject } => {
                debug!(
                    "attempt downloading all messages from NATS stream={} for subject={}",
                    stream_name, subject
                );
                self.nats
                    .subscribe(
                        stream_name,
                        subject.to_owned(),
                        DeliverPolicy::All,
                        query.id(),
                    )
                    .await
                    .map_err(|err| {
                        SyncError::Critical(format!("can't subscribe to NATS stream: {}", err))
                    })?
            }
            Query::NoSyncSubject { .. } => unreachable!("we've already returned before NoSync option"),
        };

        let nats_stream = BroadcastStream::new(nats_rx)
            .take_while(|event| {
                future::ready(!matches!(event, Ok(JetStreamEvent::InitCompleted { .. })))
            })
            .filter_map(|message| async {
                match message {
                    Ok(JetStreamEvent::Message { message, .. }) => {
                        if let Some(ref headers) = message.headers {
                            if headers.get(NATS_FROM_RHIO_HEADER).is_some() {
                                None
                            } else {
                                Some(Ok(message))
                            }
                        } else {
                            Some(Ok(message))
                        }
                    }
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
        let mut sink = into_cbor_sink(tx);
        let mut stream = into_cbor_stream(rx);

        debug!("initiator sending sync query {}", query);
        sink.send(Message::Query(query.clone())).await?;

        // @TODO(adz): This is a workaround to disable syncing in some cases as the current p2panda
        // API does not give any control to turn off syncing for some topics.
        if matches!(query, Query::NoSyncSubject { .. }) {
            sink.flush().await?;
            app_tx.flush().await?;
            return Ok(());
        }

        match query {
            Query::Bucket { .. } => todo!(),
            Query::Subject { ref subject } => {
                let stream_name = match &self.config.subscribe {
                    Some(subscriptions) => {
                        subscriptions.nats_subjects.iter().find_map(|subscription| {
                            debug!("initiator check: {} == {}", subscription.subject, subject);
                            if subscription.subject.is_matching(subject) {
                                Some(subscription.stream_name.clone())
                            } else {
                                None
                            }
                        })
                    }
                    None => None,
                }
                .expect("query matches subscription config");

                let nats_stream = self.nats_stream(stream_name, query).await?;
                let nats_stream = nats_stream.map(|event| match event {
                    Ok(message) => Ok(Hash::new(&message.payload)),
                    Err(err) => Err(err),
                });
                let nats_message_hashes: Vec<Hash> = nats_stream.try_collect().await?;

                sink.send(Message::NatsHave(nats_message_hashes)).await?;

                let Some(result) = stream.next().await else {
                    return Err(SyncError::UnexpectedBehaviour(
                        "did not receive initial topic message".into(),
                    ));
                };

                match result? {
                    Message::NatsMessages(messages) => {
                        for message in messages {
                            app_tx
                                .send(FromSync::Data(message.to_bytes(), None))
                                .await?;
                        }
                    }
                    _ => {
                        return Err(SyncError::UnexpectedBehaviour(
                            "did not receive nats messages".into(),
                        ));
                    }
                };
            }
            Query::NoSyncSubject { .. } => unreachable!(),
        }

        // Flush all bytes so that no messages are lost.
        sink.flush().await?;
        app_tx.flush().await?;

        debug!("sync session finished");

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

        let Some(result) = stream.next().await else {
            return Err(SyncError::UnexpectedBehaviour(
                "did not receive initial topic message".into(),
            ));
        };

        // This can fail in case something went wrong during CBOR decoding.
        let message = result?;

        // Expect topic as first message.
        let query = match message {
            Message::Query(query) => query,
            _ => {
                return Err(SyncError::UnexpectedBehaviour(
                    "did not receive initial topic message".into(),
                ));
            }
        };
        debug!("acceptor received sync query {}", query);

        // @TODO(adz): This is a workaround to disable syncing in some cases as the current p2panda
        // API does not give any control to turn off syncing for some topics.
        if matches!(query, Query::NoSyncSubject { .. }) {
            return Ok(());
        }

        match &query {
            Query::Bucket { .. } => todo!(),
            Query::Subject { subject } => {
                let stream_name = match &self.config.publish {
                    Some(publications) => {
                        publications.nats_subjects.iter().find_map(|publication| {
                            debug!("acceptor check: {} == {}", publication.subject, subject);
                            if publication.subject.is_matching(subject) {
                                Some(publication.stream_name.clone())
                            } else {
                                None
                            }
                        })
                    }
                    None => None,
                };

                let Some(stream_name) = stream_name else {
                    // @TODO: Inform the other peer politely that we can't provide with this
                    // subject.
                    return Ok(());
                };

                let Some(result) = stream.next().await else {
                    return Err(SyncError::UnexpectedBehaviour(
                        "did not receive initial topic message".into(),
                    ));
                };

                match result? {
                    Message::NatsHave(message_hashes) => {
                        let nats_stream = self.nats_stream(stream_name, query).await?;
                        let nats_stream = nats_stream.filter_map(|event| async {
                            match event {
                                Ok(message) => {
                                    let hash = Hash::new(&message.payload);
                                    if message_hashes.contains(&hash) {
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
                        debug!("acceptor downloaded {} NATS messages", nats_messages.len());
                        sink.send(Message::NatsMessages(nats_messages)).await?;
                    }
                    _ => {
                        return Err(SyncError::UnexpectedBehaviour(
                            "did not receive NATS have message".into(),
                        ));
                    }
                }
            }
            Query::NoSyncSubject { .. } => unreachable!("we've already returned before NoSync option"),
        }

        // Flush all bytes so that no messages are lost.
        sink.flush().await?;
        app_tx.flush().await?;

        debug!("sync session finished");

        Ok(())
    }
}
