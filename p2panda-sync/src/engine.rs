use futures::{AsyncRead, AsyncWrite, Sink};
use tokio_util::codec::{Decoder, Encoder, FramedRead, FramedWrite};
use tokio_util::compat::{FuturesAsyncReadCompatExt, FuturesAsyncWriteCompatExt};

use crate::traits::{Strategy, Sync};

pub struct SyncEngine<R, D, E> {
    pub(crate) strategy: R,
    pub(crate) decoder: D,
    pub(crate) encoder: E,
}

impl<R, D, E> SyncEngine<R, D, E> {
    pub fn new(strategy: R, decoder: D, encoder: E) -> Self {
        SyncEngine {
            strategy,
            decoder,
            encoder,
        }
    }
}

impl<S, T, M, R, D, E> Sync<S, T, M> for SyncEngine<R, D, E>
where
    D: Decoder<Item = M, Error = anyhow::Error> + Clone,
    E: Encoder<M, Error = anyhow::Error> + Clone,
    R: Strategy<S, T, M, Error = anyhow::Error>,
{
    type Error = anyhow::Error;

    async fn run(
        &mut self,
        store: &mut S,
        topic: &T,
        send: impl AsyncWrite + Unpin,
        recv: impl AsyncRead + Unpin,
        app_sink: impl Sink<M, Error = Self::Error> + Unpin,
    ) -> Result<(), Self::Error> {
        // Convert the `AsyncRead` receiver into framed (typed) `Stream`. We provide a custom
        // MessageDecoder for this purpose.
        let stream = FramedRead::new(recv.compat(), self.decoder.clone());
        let sink = FramedWrite::new(send.compat_write(), self.encoder.clone());

        self.strategy
            .sync(store, topic, stream, sink, app_sink)
            .await?;

        Ok(())
    }
}
