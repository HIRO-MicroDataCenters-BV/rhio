use std::marker::PhantomData;

use anyhow::anyhow;
use futures::{channel::mpsc::Sender, SinkExt};
use p2panda_core::{Body, Extension, Header, Operation, PublicKey};
use p2panda_store::OperationStore;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tokio_util::{bytes::Buf, codec::Decoder};

use crate::traits::{MessageHandler, ToBytes};

type LogId = String;
type SeqNum = u64;
pub type LogHeights = (PublicKey, Vec<(LogId, SeqNum)>);

#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum Message<E> {
    Have(Vec<LogHeights>),
    Operation(Header<E>, Option<Body>),
    SyncDone,
}

impl<E> ToBytes for Message<E>
where
    E: Serialize,
{
    fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        ciborium::into_writer(&self, &mut bytes).expect("type can be serialized");
        bytes
    }
}

#[derive(Clone)]
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
        let slice: &[u8] = src;
        println!("{}", slice.remaining());
        let result: Result<Message<E>, _> = ciborium::from_reader(slice);
        match result {
            // If we read the item, we also need to advance the underlying buffer.
            Ok(item) => {
                src.advance(item.to_bytes().len());
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

struct LogHeightMessageHandler<E> {
    _extension: PhantomData<E>,
}

impl<S, T, E> MessageHandler<S, T, Message<E>> for LogHeightMessageHandler<E>
where
    E: Clone + Serialize + DeserializeOwned + Extension<LogId>,
    S: OperationStore<LogId, E>,
{
    type Error = anyhow::Error;

    async fn handle_message(
        &mut self,
        store: &mut S,
        _subject: &T,
        message: Message<E>,
        rx: &mut Sender<Message<E>>,
    ) -> Result<Vec<Message<E>>, Self::Error> {
        rx.send(message.clone()).await?;
        match message {
            Message::Have(_log_heights) => {
                // @TODO:
                // Compare received log heights against local store and return
                // any operations the remote does not yet know about.
            }
            Message::Operation(header, body) => {
                let log_id: LogId = header.extract().unwrap_or(header.public_key.to_hex());
                let operation = Operation {
                    hash: header.hash(),
                    header,
                    body,
                };
                store.insert_operation(operation, log_id)?;
            }
            Message::SyncDone => return Ok(vec![]),
        }
        Ok(vec![])
    }
}

#[cfg(test)]
mod tests {
    use std::marker::PhantomData;

    use futures::channel::mpsc::channel;
    use futures::StreamExt;
    use p2panda_core::{Body, Extension, Header, PrivateKey};
    use p2panda_store::MemoryStore;
    use serde::{Deserialize, Serialize};

    use super::{LogHeightMessageHandler, LogId, MessageDecoder};
    use crate::engine::SyncEngine;
    use crate::log_height::Message;
    use crate::traits::{Sync, ToBytes};

    #[derive(Clone, Debug, Serialize, Deserialize)]
    pub struct LogHeightExtensions {
        log_id: LogId,
    }

    impl Extension<LogId> for LogHeightExtensions {
        fn extract(&self) -> Option<LogId> {
            Some(self.log_id.clone())
        }
    }

    #[tokio::test]
    async fn basic() {
        let mut sync = SyncEngine {
            store: MemoryStore::<LogId, LogHeightExtensions>::new(),
            decoder: MessageDecoder {
                _extension: PhantomData {},
            },
            message_handler: LogHeightMessageHandler {
                _extension: PhantomData {},
            },
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

        let (mut tx, mut rx) = channel(128);

        let send = Vec::new();
        let recv: Vec<u8> = vec![
            Message::<LogHeightExtensions>::Have(vec![]).to_bytes(),
            message.to_bytes(),
            Message::<LogHeightExtensions>::SyncDone.to_bytes(),
        ]
        .concat();

        sync.run(&String::from("my_topic"), send, &recv[..], &mut tx)
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
