use std::marker::PhantomData;

use anyhow::anyhow;
use futures::{Sink, SinkExt, Stream, StreamExt};
use futures::channel::mpsc::Sender;
use p2panda_core::{Body, Extension, Header, Operation, PublicKey};
use p2panda_store::OperationStore;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tokio_util::{
    bytes::Buf,
    codec::{Decoder, Encoder},
};

use crate::traits::Strategy;

type LogId = String;
type SeqNum = u64;
pub type LogHeights = (PublicKey, Vec<(LogId, SeqNum)>);

#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum Message<E> {
    Have(Vec<LogHeights>),
    Operation(Header<E>, Option<Body>),
    SyncDone,
}

#[cfg(test)]
impl<E> Message<E>
where
    E: Serialize,
{
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        ciborium::into_writer(&self, &mut bytes).expect("type can be serialized");
        bytes
    }
}

#[derive(Clone, Default)]
struct MessageEncoder<E> {
    _extension: PhantomData<E>,
}

impl<E> Encoder<Message<E>> for MessageEncoder<E>
where
    E: Serialize,
{
    type Error = anyhow::Error;

    fn encode(
        &mut self,
        item: Message<E>,
        dst: &mut tokio_util::bytes::BytesMut,
    ) -> Result<(), Self::Error> {
        let mut bytes = Vec::new();
        ciborium::into_writer(&item, &mut bytes)?;
        dst.extend_from_slice(&bytes);
        Ok(())
    }
}

#[derive(Clone, Default)]
struct MessageDecoder<E> {
    _extension: PhantomData<E>,
}

impl<E> Decoder for MessageDecoder<E>
where
    E: Serialize + DeserializeOwned,
{
    type Item = Message<E>;

    type Error = anyhow::Error;

    fn decode(
        &mut self,
        src: &mut tokio_util::bytes::BytesMut,
    ) -> Result<Option<Self::Item>, Self::Error> {
        let reader = src.reader();
        let result: Result<Message<E>, _> = ciborium::from_reader(reader);
        match result {
            // If we read the item, we also need to advance the underlying buffer.
            Ok(item) => {
                return Ok(Some(item));
            }
            Err(ref error) => match error {
                // Sometimes the EOF is signalled as IO error
                ciborium::de::Error::Io(_) => return Ok(None),
                e => {
                    return Err(anyhow!("{e}"));
                }
            },
        }
    }
}

struct LogHeightStrategy;

impl<S, T, E> Strategy<S, T, Message<E>> for LogHeightStrategy
where
    E: Clone + Serialize + DeserializeOwned + Extension<LogId>,
    S: OperationStore<LogId, E>,
{
    type Error = anyhow::Error;

    async fn sync(
        &mut self,
        store: &mut S,
        _topic: &T,
        mut stream: impl Stream<Item = Result<Message<E>, Self::Error>> + Unpin,
        mut sink: impl Sink<Message<E>, Error = Self::Error> + Unpin,
        mut app_sink: impl Sink<Message<E>, Error = Self::Error> + Unpin,
    ) -> Result<(), Self::Error> {
        while let Some(result) = stream.next().await {
            let message = result?;

            let replies = match &message {
                Message::Have(_log_heights) => {
                    // @TODO:
                    // Compare received log heights against local store and return
                    // any operations the remote does not yet know about.
                    vec![]
                }
                Message::Operation(header, body) => {
                    let log_id: LogId = header.extract().unwrap_or(header.public_key.to_hex());
                    let operation = Operation {
                        hash: header.hash(),
                        header: header.clone(),
                        body: body.clone(),
                    };
                    store.insert_operation(operation, log_id)?;
                    vec![]
                }
                Message::SyncDone => vec![],
            };

            app_sink.send(message).await?;

            for message in replies {
                sink.send(message).await?;
            }
        }

        sink.close().await?;
        app_sink.close().await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use futures::channel::mpsc::channel;
    use futures::{SinkExt, StreamExt};
    use p2panda_core::{Body, Extension, Header, PrivateKey};
    use p2panda_store::MemoryStore;
    use serde::{Deserialize, Serialize};

    use super::{LogHeightStrategy, LogId, MessageDecoder};
    use crate::engine::SyncEngine;
    use crate::log_height::{Message, MessageEncoder};
    use crate::traits::Sync;

    #[derive(Clone, Debug, Default, Serialize, Deserialize)]
    pub struct LogHeightExtensions {
        log_id: Option<LogId>,
    }

    impl Extension<LogId> for LogHeightExtensions {
        fn extract(&self) -> Option<LogId> {
            self.log_id.clone()
        }
    }

    #[tokio::test]
    async fn basic() {
        let mut store = MemoryStore::<LogId, LogHeightExtensions>::new();
        let decoder = MessageDecoder::default();
        let encoder = MessageEncoder::default();
        let strategy = LogHeightStrategy {};
        let mut sync = SyncEngine {
            strategy,
            decoder,
            encoder,
        };

        let private_key = PrivateKey::new();

        let body = Body::new("Hello, Sloth!".as_bytes());

        let mut header = Header {
            version: 1,
            public_key: private_key.public_key(),
            signature: None,
            payload_size: body.size(),
            payload_hash: Some(body.hash()),
            timestamp: 0,
            seq_num: 0,
            backlink: None,
            previous: vec![],
            extensions: None::<LogHeightExtensions>,
        };

        header.sign(&private_key);

        let message = Message::Operation(header, Some(body));

        let (tx, mut rx) = channel(128);

        let send = Vec::new();
        let recv: Vec<u8> = vec![
            Message::<LogHeightExtensions>::Have(vec![]).to_bytes(),
            message.to_bytes(),
            Message::<LogHeightExtensions>::SyncDone.to_bytes(),
        ]
        .concat();

        sync.run(
            &mut store,
            &String::from("my_topic"),
            send,
            &recv[..],
            tx.sink_map_err(anyhow::Error::from),
        )
        .await
        .unwrap();

        assert_eq!(
            rx.next().await.unwrap().to_bytes(),
            Message::<LogHeightExtensions>::Have(vec![]).to_bytes()
        );
        assert_eq!(rx.next().await.unwrap().to_bytes(), message.to_bytes());
        assert_eq!(
            rx.next().await.unwrap().to_bytes(),
            Message::<LogHeightExtensions>::SyncDone.to_bytes()
        );
    }
}
