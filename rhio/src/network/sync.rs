use std::sync::Arc;

use async_trait::async_trait;
use futures_util::{AsyncRead, AsyncWrite, Sink, SinkExt};
use p2panda_sync::cbor::{into_cbor_sink, into_cbor_stream};
use p2panda_sync::{FromSync, SyncError, SyncProtocol};
use serde::{Deserialize, Serialize};
use tokio_stream::StreamExt;
use tracing::debug;

use crate::nats::Nats;
use crate::topic::Query;

static SYNC_PROTOCOL_NAME: &str = "rhio-sync-v1";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Message {
    Query(Query),
}

#[derive(Clone, Debug)]
pub struct RhioSyncProtocol {
    nats: Nats,
}

impl RhioSyncProtocol {
    pub fn new(nats: Nats) -> Self {
        Self { nats }
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
        let mut sync_done_received = false;

        let mut sink = into_cbor_sink(tx);
        // let mut stream = into_cbor_stream(rx);

        sink.send(Message::Query(query.clone())).await?;

        // @TODO(adz): This is a workaround to disable syncing in some cases as the current p2panda
        // API does not give any control to turn off syncing for some topics.
        if matches!(query, Query::NoSync { .. }) {
            sink.flush().await?;
            app_tx.flush().await?;
            return Ok(());
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
        let mut sync_done_sent = false;
        let mut sync_done_received = false;

        // let mut sink = into_cbor_sink(tx);
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

        // @TODO(adz): This is a workaround to disable syncing in some cases as the current p2panda
        // API does not give any control to turn off syncing for some topics.
        if matches!(query, Query::NoSync { .. }) {
            return Ok(());
        }

        // Flush all bytes so that no messages are lost.
        // sink.flush().await?;
        app_tx.flush().await?;

        debug!("sync session finished");

        Ok(())
    }
}
