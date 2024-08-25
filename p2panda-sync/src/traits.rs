use std::fmt::Debug;

use futures::channel::mpsc::Sender;
use futures::{AsyncRead, AsyncWrite, Stream};

pub trait ToBytes {
    fn to_bytes(&self) -> Vec<u8>;
}

pub trait Sync<S, T, M> {
    type Error: Debug;

    /// Run a full sync session over a "topic".
    ///
    /// Accepts a sender and receiver which implement (`AsyncWrite` and `AsyncRead`) respectively.
    /// For example [quinn::SendStream](https://docs.rs/quinn/latest/quinn/struct.SendStream.html) and
    /// [quin::RecvStream](https://docs.rs/quinn/latest/quinn/struct.RecvStream.html) could be
    /// passed into this method. The rx side of a mpsc channel is also taken so that new messages
    /// received through the sync protocol can be sent on to the application.
    async fn run(
        &mut self,
        store: &mut S,
        topic: &T,
        send: impl AsyncWrite + Unpin + 'static,
        recv: impl AsyncRead + Unpin,
        rx: &mut Sender<M>,
    ) -> Result<(), Self::Error>;
}

pub trait Strategy<S, T, M> {
    type Error: Debug;

    async fn sync(
        &mut self,
        store: &mut S,
        topic: &T,
        recv: impl Stream<Item = Result<M, anyhow::Error>> + Unpin,
        reply_tx: &mut Sender<M>,
        app_tx: &mut Sender<M>
    ) -> Result<(), Self::Error>;
}
