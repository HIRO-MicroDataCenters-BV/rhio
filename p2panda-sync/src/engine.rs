use futures::channel::mpsc::Sender;
use futures::io::BufWriter;
use futures::{AsyncRead, AsyncWrite, AsyncWriteExt, TryStreamExt};
use tokio_stream::StreamExt;
use tokio_util::codec::{Decoder, FramedRead};
use tokio_util::compat::FuturesAsyncReadCompatExt;

use crate::traits::{MessageHandler, Sync, ToBytes};

pub struct SyncEngine<S, D, H> {
    pub(crate) store: S,
    pub(crate) decoder: D,
    pub(crate) message_handler: H,
}

impl<T, M, S, D, H> Sync<T, M> for SyncEngine<S, D, H>
where
    M: ToBytes,
    D: Decoder<Item = M, Error = anyhow::Error> + Clone,
    H: MessageHandler<S, T, M, Error = anyhow::Error>,
{
    type Error = anyhow::Error;

    async fn run(
        &mut self,
        subject: &T,
        send: impl AsyncWrite + Unpin,
        recv: impl AsyncRead + Unpin,
        rx: &mut Sender<M>,
    ) -> Result<(), Self::Error> {
        // Convert the `AsyncRead` receiver into framed (typed) `Stream`. We provide a custom
        // MessageDecoder for this purpose.
        let mut stream_reader = FramedRead::new(recv.compat(), self.decoder.clone()).into_stream();

        // Wrap the `AsyncWrite` in `BufWriter`` which has ergonomic and efficiency benefits: https://docs.rs/tokio/latest/tokio/io/struct.BufWriter.html
        let mut writer = BufWriter::new(send);

        while let Some(result) = stream_reader.next().await {
            let message = result.expect("invalid message received");

            // Handle the message
            //
            // This method will contain all the sync logic, has access the store, and returns
            // all messages we want to send back on the send stream.
            let messages = self
                .message_handler
                .handle_message(&mut self.store, subject, message, rx)
                .await?;

            // Write all "reply" messages into the send stream.
            for message in messages {
                writer.write(&message.to_bytes()[..]).await?;
            }
        }

        Ok(())
    }
}
