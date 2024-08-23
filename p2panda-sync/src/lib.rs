use futures::channel::mpsc::Sender;
use futures::io::{AsyncRead, AsyncWrite};

trait Sync<T> {
    type Message;
    type Error;

    /// Run a full sync session over a "subject".
    ///
    /// Accepts a sender and receiver which implement (`AsyncWrite` and `AsyncRead`) respectively.
    /// For example [quinn::SendStream](https://docs.rs/quinn/latest/quinn/struct.SendStream.html) and
    /// [quin::RecvStream](https://docs.rs/quinn/latest/quinn/struct.RecvStream.html) could be
    /// passed into this method. The rx side of a mpsc channel is also taken so that new messages
    /// received through the sync protocol can be sent on to the application.
    async fn sync(
        &mut self,
        subject: &T,
        send: impl AsyncWrite + Unpin,
        recv: impl AsyncRead + Unpin,
        rx: &Sender<Self::Message>,
    ) -> Result<(), Self::Error>;

    /// Handle a single sync message.
    ///
    /// Returns all messages we wish to back to the remote peer.
    async fn handle_message(
        &mut self,
        subject: &T,
        message: Self::Message,
        rx: &Sender<Self::Message>,
    ) -> Result<Vec<Self::Message>, Self::Error>;
}

#[cfg(test)]
mod tests {
    use futures::channel::mpsc::Sender;
    use futures::io::BufWriter;
    use futures::{AsyncRead, AsyncWrite, AsyncWriteExt, TryStreamExt};
    use p2panda_core::{Body, Header, PublicKey};
    use p2panda_store::MemoryStore;
    use serde::{Deserialize, Serialize};
    use tokio_stream::StreamExt;
    use tokio_util::codec::{Decoder, FramedRead};
    use tokio_util::compat::FuturesAsyncReadCompatExt;

    use super::Sync;

    struct LogHeightSync<E> {
        store: MemoryStore<LogId, E>,
    }

    type LogId = String;
    type SeqNum = u64;
    pub type LogHeights = (PublicKey, Vec<(LogId, SeqNum)>);

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub enum Message {
        Have(Vec<LogHeights>),
        Operation(Header, Body),
        SyncDone,
    }

    struct MessageDecoder {}

    impl Decoder for MessageDecoder {
        type Item = Message;

        type Error = anyhow::Error;

        fn decode(
            &mut self,
            src: &mut tokio_util::bytes::BytesMut,
        ) -> Result<Option<Self::Item>, Self::Error> {
            // do some decoding here, need to learn more about how exactly this is done.
            todo!()
        }
    }

    impl Sync<String> for LogHeightSync<()> {
        type Message = Message;

        type Error = anyhow::Error;

        async fn sync(
            &mut self,
            subject: &String,
            send: impl AsyncWrite + Unpin,
            recv: impl AsyncRead + Unpin,
            rx: &Sender<Message>,
        ) -> Result<(), Self::Error> {
            // Convert the `AsyncRead` receiver into framed (typed) `Stream`. We provide a custom
            // MessageDecoder for this purpose.
            let decoder = MessageDecoder {};
            let mut stream_reader = FramedRead::new(recv.compat(), decoder).into_stream();

            // Wrap the `AsyncWrite` in BufWriter which has ergonomic and efficiency benefits: https://docs.rs/tokio/latest/tokio/io/struct.BufWriter.html
            let mut writer = BufWriter::new(send);

            while let Some(result) = stream_reader.next().await {
                let message = result.expect("invalid message received");

                // Handle the message
                //
                // This method will contain all the sync logic, has access the store, and returns
                // all messages we want to send back on the send stream.
                let messages = self.handle_message(subject, message, &rx).await?;
                for _message in messages {
                    // @TODO: Write the message bytes to the `BufWriter`
                    // writer.write(message.to_bytes()).await?;
                    writer.write(&[0, 1, 2, 3]).await?;
                }
            }

            Ok(())
        }

        async fn handle_message(
            &mut self,
            subject: &String,
            message: Self::Message,
            rx: &Sender<Self::Message>,
        ) -> Result<Vec<Self::Message>, Self::Error> {
            // Handle messages arriving from the remote peer.
            //
            // Return any messages we want to send back to them.
            todo!()
        }
    }
}
