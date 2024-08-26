use futures::{AsyncRead, AsyncWrite, StreamExt};
use tokio_util::codec::{Decoder, Encoder, FramedRead, FramedWrite};
use tokio_util::compat::{FuturesAsyncReadCompatExt, FuturesAsyncWriteCompatExt};

use crate::traits::{Strategy, Sync};

pub struct SyncEngine<S, R, D, E> {
    pub(crate) store: S,
    pub(crate) strategy: R,
    pub(crate) decoder: D,
    pub(crate) encoder: E,
}

impl<S, R, D, E> SyncEngine<S, R, D, E> {
    pub fn new(store: S, strategy: R, decoder: D, encoder: E) -> Self {
        SyncEngine {
            store,
            strategy,
            decoder,
            encoder,
        }
    }
}

impl<S, T, M, R, D, E> Sync<T, M> for SyncEngine<S, R, D, E>
where
    R: Strategy<S, T, M, Error = anyhow::Error>,
    D: Decoder<Item = M, Error = anyhow::Error> + Clone,
    E: Encoder<M, Error = anyhow::Error> + Clone,
{
    type Error = anyhow::Error;

    async fn run(
        &mut self,
        topic: &T,
        send: impl AsyncWrite + Unpin,
        recv: impl AsyncRead + Unpin,
    ) -> Result<(), Self::Error> {
        // Convert the `AsyncRead` and `AsyncWrite` into framed (typed) `Stream` and `Sink`. We provide a custom
        // `tokio_util::codec::Decoder` and `tokio_util::codec::Encoder` for this purpose.
        let stream = FramedRead::new(recv.compat(), self.decoder.clone()).fuse();
        let sink = FramedWrite::new(send.compat_write(), self.encoder.clone());

        self.strategy
            .sync(&mut self.store, topic, stream, sink)
            .await?;

        Ok(())
    }
}
