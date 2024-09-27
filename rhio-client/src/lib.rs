use anyhow::Result;
use async_nats::jetstream::Context;
use p2panda_core::{Hash, PrivateKey};
use p2panda_store::MemoryStore;
use rhio_core::{create_blob_announcement, create_message, encode_operation, RhioExtensions};

pub struct Client {
    jetstream: JetStream,
    store: MemoryStore<[u8; 32], RhioExtensions>,
    private_key: PrivateKey,
}

impl Client {
    pub fn new(private_key: PrivateKey, jetstream: JetStream) -> Self {
        Self {
            jetstream,
            private_key,
            store: MemoryStore::new(),
        }
    }

    pub async fn new_ephemeral(endpoint: &str) -> Result<Self> {
        Ok(Self {
            jetstream: JetStream::new(endpoint).await?,
            store: MemoryStore::new(),
            private_key: PrivateKey::new(),
        })
    }

    pub async fn publish(&mut self, subject: String, topic: String, payload: &[u8]) -> Result<()> {
        let operation = create_message(
            &mut self.store,
            &self.private_key,
            &subject,
            &topic,
            payload,
        )
        .await?;
        let encoded_operation = encode_operation(operation.header, operation.body)?;
        self.jetstream.publish(subject, encoded_operation).await?;
        Ok(())
    }

    pub async fn announce_blob(
        &mut self,
        subject: String,
        topic: String,
        hash: Hash,
    ) -> Result<()> {
        let operation =
            create_blob_announcement(&mut self.store, &self.private_key, &subject, &topic, hash)
                .await?;
        let encoded_operation = encode_operation(operation.header, operation.body)?;
        self.jetstream.publish(subject, encoded_operation).await?;
        Ok(())
    }
}

pub struct JetStream {
    context: Context,
}

impl JetStream {
    pub async fn new(endpoint: &str) -> Result<Self> {
        let client = async_nats::connect(endpoint).await?;
        let context = async_nats::jetstream::new(client);
        Ok(Self { context })
    }

    pub async fn publish(&self, subject: String, payload: Vec<u8>) -> Result<()> {
        self.context.publish(subject, payload.into()).await?;
        Ok(())
    }
}
