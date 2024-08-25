use futures::channel::mpsc::{channel, Sender};
use futures::io::BufWriter;
use futures::{AsyncRead, AsyncWrite, AsyncWriteExt, TryStreamExt};
use tokio_stream::StreamExt;
use tokio_util::codec::{Decoder, FramedRead};
use tokio_util::compat::FuturesAsyncReadCompatExt;

use crate::traits::{Strategy, Sync, ToBytes};

pub struct SyncEngine<R, D> {
    pub(crate) strategy: R,
    pub(crate) decoder: D,
}

impl<R, D> SyncEngine<R, D> {
    pub fn new(strategy: R, decoder: D) -> Self {
        SyncEngine { decoder, strategy }
    }
}

impl<S, T, M, R, D> Sync<S, T, M> for SyncEngine<R, D>
where
    M: ToBytes + 'static,
    D: Decoder<Item = M, Error = anyhow::Error> + Clone,
    R: Strategy<S, T, M, Error = anyhow::Error>,
{
    type Error = anyhow::Error;

    async fn run(
        &mut self,
        store: &mut S,
        topic: &T,
        send: impl AsyncWrite + Unpin + 'static,
        recv: impl AsyncRead + Unpin,
        app_tx: &mut Sender<M>,
    ) -> Result<(), Self::Error> {
        // Convert the `AsyncRead` receiver into framed (typed) `Stream`. We provide a custom
        // MessageDecoder for this purpose.
        let stream_reader = FramedRead::new(recv.compat(), self.decoder.clone()).into_stream();

        // Wrap the `AsyncWrite` in `BufWriter`` which has ergonomic and efficiency benefits: https://docs.rs/tokio/latest/tokio/io/struct.BufWriter.html
        let mut writer = BufWriter::new(send);

        let (mut tx, mut rx) = channel::<M>(128);

        let fut_1 = async move {
            while let Some(message) = rx.next().await {
                writer.write(&message.to_bytes()[..]).await?;
            }
            Ok::<_, Self::Error>(())
        };

        let fut_2 = self
            .strategy
            .sync(store, topic, stream_reader, &mut tx, app_tx);

        let (res_1, res_2) = tokio::join!(fut_1, fut_2);

        res_1?;
        res_2?;

        Ok(())
    }
}
