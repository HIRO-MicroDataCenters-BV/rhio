use anyhow::Result;
use async_nats::jetstream::Context;
use p2panda_core::PrivateKey;
use p2panda_store::MemoryStore;
use rhio_core::{create_operation, encode_operation, LogId, RhioExtensions};

pub struct Client {
    jetstream: JetStream,
    store: MemoryStore<LogId, RhioExtensions>,
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

    pub async fn publish(&mut self, subject: String, payload: &[u8]) -> Result<()> {
        let operation = create_operation(&mut self.store, &self.private_key, &subject, payload)?;
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
