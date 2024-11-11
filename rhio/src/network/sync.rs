use std::sync::Arc;

use async_trait::async_trait;
use futures_util::{AsyncRead, AsyncWrite, Sink, SinkExt};
use p2panda_sync::cbor::{into_cbor_sink, into_cbor_stream};
use p2panda_sync::{FromSync, SyncError, SyncProtocol, Topic};
use tokio::sync::mpsc;
use tracing::debug;

use crate::nats::Nats;

static SYNC_PROTOCOL_NAME: &str = "rhio-sync-v1";

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
impl<'a, T> SyncProtocol<T, 'a> for RhioSyncProtocol
where
    T: Topic,
{
    fn name(&self) -> &'static str {
        SYNC_PROTOCOL_NAME
    }

    async fn initiate(
        self: Arc<Self>,
        topic: T,
        tx: Box<&'a mut (dyn AsyncWrite + Send + Unpin)>,
        rx: Box<&'a mut (dyn AsyncRead + Send + Unpin)>,
        mut app_tx: Box<&'a mut (dyn Sink<FromSync<T>, Error = SyncError> + Send + Unpin)>,
    ) -> Result<(), SyncError> {
        let mut sync_done_received = false;

        // let mut sink = into_cbor_sink(tx);
        // let mut stream = into_cbor_stream(rx);

        // Flush all bytes so that no messages are lost.
        // sink.flush().await?;
        // app_tx.flush().await?;

        debug!("sync session finished");

        Ok(())
    }

    async fn accept(
        self: Arc<Self>,
        tx: Box<&'a mut (dyn AsyncWrite + Send + Unpin)>,
        rx: Box<&'a mut (dyn AsyncRead + Send + Unpin)>,
        mut app_tx: Box<&'a mut (dyn Sink<FromSync<T>, Error = SyncError> + Send + Unpin)>,
    ) -> Result<(), SyncError> {
        let mut sync_done_sent = false;
        let mut sync_done_received = false;

        // let mut sink = into_cbor_sink(tx);
        // let mut stream = into_cbor_stream(rx);

        // Flush all bytes so that no messages are lost.
        // sink.flush().await?;
        // app_tx.flush().await?;

        debug!("sync session finished");

        Ok(())
    }
}
