use std::marker::PhantomData;

use anyhow::anyhow;
use futures::{Sink, SinkExt, Stream, StreamExt};
use p2panda_core::extensions::DefaultExtensions;
use p2panda_core::{Body, Extension, Header, Operation, PublicKey};
use p2panda_store::{LogStore, OperationStore};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tokio_util::{
    bytes::Buf,
    codec::{Decoder, Encoder},
};

use crate::traits::Strategy;

type LogId = String;
type SeqNum = u64;
pub type LogHeights = Vec<(PublicKey, SeqNum)>;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum Message<E = DefaultExtensions> {
    Have(LogHeights),
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
pub struct MessageEncoder<E> {
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
pub struct MessageDecoder<E> {
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

pub struct LogHeightStrategy;

impl<S, E> Strategy<S, LogId, Message<E>> for LogHeightStrategy
where
    S: OperationStore<LogId, E> + LogStore<LogId, E>,
    E: Clone + std::fmt::Debug + Serialize + DeserializeOwned + Extension<LogId>,
{
    type Error = anyhow::Error;

    async fn sync(
        &mut self,
        store: &mut S,
        topic: &LogId, // topic is assumed to be equivalent to the log id we are interested in
        mut stream: impl Stream<Item = Result<Message<E>, Self::Error>>
            + Unpin
            + futures::stream::FusedStream,
        mut sink: impl Sink<Message<E>, Error = Self::Error> + Unpin,
    ) -> Result<(), Self::Error> {
        while let Some(result) = stream.next().await {
            let message = result?;

            let replies = match &message {
                Message::Have(log_heights) => {
                    // @TODO: We need a new method on the store for getting all logs (or maybe
                    // just their heights?) by their log id so that we can send back any logs the
                    // remote peer doesn't know anything about.

                    let mut messages = vec![];
                    for (public_key, seq_num) in log_heights {
                        let local_seq_num = store
                            .latest_operation(*public_key, topic.to_string())?
                            .map(|operation| operation.header.seq_num)
                            .unwrap_or(0);
                        if *seq_num >= local_seq_num {
                            continue;
                        }
                        let mut log = store.get_log(*public_key, topic.to_string())?;
                        log.split_off(*seq_num as usize + 1)
                            .into_iter()
                            .for_each(|operation| {
                                messages.push(Message::Operation(operation.header, operation.body))
                            });
                    }
                    messages
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

            // @TODO: we'd rather process all messages at once using `send_all`. For this
            // we need to turn `replies` into a stream.
            for message in replies {
                sink.send(message).await?;
            }

            // If we received a `SyncDone` message then we can break out of this loop.
            if let Message::SyncDone = message {
                break;
            }
        }

        // We have processed all messages on the stream so can now send a final `SyncDone` on the sink
        sink.send(Message::SyncDone).await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use p2panda_core::extensions::DefaultExtensions;
    use p2panda_core::{Body, Hash, Header, Operation, PrivateKey};
    use p2panda_store::{MemoryStore, OperationStore};
    use serde::Serialize;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

    use super::{LogHeightStrategy, MessageDecoder};
    use crate::engine::SyncEngine;
    use crate::log_height::{Message, MessageEncoder};
    use crate::traits::Sync;

    fn generate_operation<E: Clone + Serialize>(
        private_key: &PrivateKey,
        body: Body,
        seq_num: u64,
        timestamp: u64,
        backlink: Option<Hash>,
        extensions: Option<E>,
    ) -> Operation<E> {
        let mut header = Header {
            version: 1,
            public_key: private_key.public_key(),
            signature: None,
            payload_size: body.size(),
            payload_hash: Some(body.hash()),
            timestamp,
            seq_num,
            backlink,
            previous: vec![],
            extensions,
        };
        header.sign(&private_key);

        Operation {
            hash: header.hash(),
            header,
            body: Some(body),
        }
    }

    #[tokio::test]
    async fn basic() {
        const TOPIC_ID: &str = "my_topic";

        // Setup store with 3 operations in it
        let mut store = MemoryStore::default();
        let private_key = PrivateKey::new();

        let body = Body::new("Hello, Sloth!".as_bytes());
        let operation0 = generate_operation(&private_key, body.clone(), 0, 0, None, None);
        let operation1 = generate_operation(
            &private_key,
            body.clone(),
            1,
            100,
            Some(operation0.hash),
            None,
        );
        let operation2 = generate_operation(
            &private_key,
            body.clone(),
            2,
            200,
            Some(operation1.hash),
            None,
        );

        // Insert these operations to the store using `TOPIC_ID` as the log id
        store
            .insert_operation(operation0.clone(), TOPIC_ID.to_string())
            .unwrap();
        store
            .insert_operation(operation1.clone(), TOPIC_ID.to_string())
            .unwrap();
        store
            .insert_operation(operation2.clone(), TOPIC_ID.to_string())
            .unwrap();

        // Compose the `SyncEngine`
        let mut sync = SyncEngine {
            store,
            strategy: LogHeightStrategy,
            decoder: MessageDecoder::default(),
            encoder: MessageEncoder::default(),
        };

        // Create a duplex stream which simulate both ends of a bi-directional network connection
        let (peer_a, mut peer_b) = tokio::io::duplex(64 * 1024);
        let (peer_a_read, peer_a_write) = tokio::io::split(peer_a);

        // Write some message into peer_b's send buffer
        let message1: Message<DefaultExtensions> =
            Message::Have(vec![(private_key.public_key(), 0)]);
        let message2: Message<DefaultExtensions> = Message::SyncDone;
        let message_bytes = vec![message1.to_bytes(), message2.to_bytes()].concat();
        peer_b.write_all(&message_bytes[..]).await.unwrap();

        // Run the sync session (which consumes the above messages)
        sync.run(
            // We specify the topic id here
            &TOPIC_ID.to_string(),
            peer_a_write.compat_write(),
            peer_a_read.compat(),
        )
        .await
        .unwrap();

        // Read the entire buffer out of peer_b's read stream
        let mut buf = Vec::new();
        peer_b.read_to_end(&mut buf).await.unwrap();

        // It should contain the following two sync messages (these are the ones peer_b is missing)
        let received_message1 =
            Message::Operation(operation1.header.clone(), operation1.body.clone());
        let received_message2 =
            Message::Operation(operation2.header.clone(), operation2.body.clone());
        let receive_message3 = Message::<DefaultExtensions>::SyncDone;
        assert_eq!(
            buf,
            [
                received_message1.to_bytes(),
                received_message2.to_bytes(),
                receive_message3.to_bytes()
            ]
            .concat()
        );
    }
}
