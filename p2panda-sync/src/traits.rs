use futures::channel::mpsc::Sender;
use futures::{AsyncRead, AsyncWrite};

pub trait ToBytes {
    fn to_bytes(&self) -> Vec<u8>;
}

pub trait Sync<T, M> {
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
        rx: &mut Sender<M>,
    ) -> Result<(), Self::Error>;
}

pub trait MessageHandler<S, T, M> {
    type Error;

    async fn handle_message(
        &mut self,
        store: &mut S,
        subject: &T,
        message: M,
        rx: &mut Sender<M>,
    ) -> Result<Vec<M>, Self::Error>;
}
