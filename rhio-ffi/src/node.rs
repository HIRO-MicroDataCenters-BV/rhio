use std::sync::{Arc, Mutex};

use futures::future::BoxFuture;
use futures::FutureExt;
use rhio::node::TopicSender;
use rhio::private_key::generate_ephemeral_private_key;
use rhio::Node as RhioNode;
use tracing::warn;

use crate::config::Config;
use crate::error::{CallbackError, RhioError};
use crate::messages::{Message, MessageMeta};
use crate::topic_id::TopicId;
use crate::types::{Hash, ImportPath, Path, PublicKey, SocketAddr};

/// Network node which handles connecting to known/discovered peers, gossiping p2panda operations
/// over topics and syncing blob data using the BAO protocol.
#[derive(uniffi::Object)]
pub struct Node {
    pub inner: RhioNode,
}

#[uniffi::export]
impl Node {
    /// Configure and spawn a node.
    #[uniffi::constructor(async_runtime = "tokio")]
    pub async fn spawn(config: &Config) -> Result<Self, RhioError> {
        let private_key = generate_ephemeral_private_key();
        let rhio_node = RhioNode::spawn(config.clone().into(), private_key).await?;
        Ok(Self { inner: rhio_node })
    }

    /// Returns the PublicKey of this node which is used as it's unique network id.
    ///
    /// This ID is the unique addressing information of this node and other peers must know it to
    /// be able to connect to this node.
    #[uniffi::method]
    pub fn id(&self) -> PublicKey {
        self.inner.id().into()
    }

    /// Returns the direct addresses of this Node.
    ///
    /// The direct addresses of the Node are those that could be used by other nodes
    /// to establish direct connectivity, depending on the network situation. The yielded lists of
    /// direct addresses contain both the locally-bound addresses and the Node's publicly
    /// reachable addresses discovered through mechanisms such as STUN and port mapping. Hence
    /// usually only a subset of these will be applicable to a certain remote node.
    #[uniffi::method(async_runtime = "tokio")]
    pub async fn direct_addresses(&self) -> Option<Vec<SocketAddr>> {
        self.inner
            .direct_addresses()
            .await
            .map(|addrs| addrs.into_iter().map(SocketAddr::from).collect())
    }

    /// Import a blob from either a path or a URL and export it to the minio store.
    ///
    /// Add a blob from a path on the local filesystem to the dedicated blob store and
    /// make it available on the network identified by it's Blake3 hash.
    #[uniffi::method(async_runtime = "tokio")]
    pub async fn import_blob(&self, import_path: ImportPath) -> Result<Hash, RhioError> {
        let hash = self.inner.import_blob(import_path.into()).await?;
        Ok(hash.into())
    }

    /// Export a blob to the filesystem.
    ///
    /// Copies an existing blob from the blob store to a location on the filesystem.
    #[uniffi::method(async_runtime = "tokio")]
    pub async fn export_blob_filesystem(&self, hash: Hash, path: Path) -> Result<(), RhioError> {
        self.inner
            .export_blob_filesystem(hash.into(), path.into())
            .await?;
        Ok(())
    }

    /// Export a blob to a minio bucket.
    ///
    /// Copies an existing blob from the blob store to the provided minio bucket.
    #[uniffi::method(async_runtime = "tokio")]
    pub async fn export_blob_minio(
        &self,
        hash: Hash,
        region: String,
        endpoint: String,
        bucket_name: String,
    ) -> Result<(), RhioError> {
        self.inner
            .export_blob_minio(hash.into(), region, endpoint, bucket_name)
            .await?;
        Ok(())
    }

    /// Download a blob from the network and export it to a minio store.
    ///
    /// Attempt to download a blob from peers on the network and place it into the nodes blob store.
    #[uniffi::method(async_runtime = "tokio")]
    pub async fn download_blob(&self, hash: Hash) -> Result<(), RhioError> {
        self.inner.download_blob(hash.into()).await?;
        Ok(())
    }

    /// Subscribe to a gossip topic.
    ///
    /// Accepts a callback method which should be used to handle messages arriving on this topic.
    /// Returns a sender which can then be used to broadcast events to all peers also subscribed
    /// to this topic. The sender can be awaited using its `ready()` method which only resolves
    /// when at least one other peers subscribes to the same topic.
    #[uniffi::method(async_runtime = "tokio")]
    pub async fn subscribe(
        &self,
        topic: &TopicId,
        cb: Arc<dyn GossipMessageCallback>,
    ) -> Result<Sender, RhioError> {
        let (topic_tx, mut topic_rx, ready) = self.inner.subscribe(topic.to_owned().into()).await?;

        let sender = Sender {
            inner: topic_tx,
            ready_fut: Mutex::new(Some(ready.boxed())),
        };

        tokio::task::spawn(async move {
            while let Ok((message, meta)) = topic_rx.recv().await {
                if let Err(err) = cb
                    .on_message(Arc::new(message.into()), Arc::new(MessageMeta(meta)))
                    .await
                {
                    warn!("cb error, gossip: {:?}", err);
                }
            }
        });

        Ok(sender)
    }
}

/// Callback used to handle all incoming messages on a particular topic.
///
/// As well as the message content itself, additional information about the message is passed into
/// the callback in the meta parameter.
#[uniffi::export(with_foreign)]
#[async_trait::async_trait]
pub trait GossipMessageCallback: Send + Sync + 'static {
    async fn on_message(
        &self,
        msg: Arc<Message>,
        meta: Arc<MessageMeta>,
    ) -> Result<(), CallbackError>;
}

/// Channel for broadcasting messages to all peers subscribed to a particular topic.
#[derive(uniffi::Object)]
pub struct Sender {
    pub(crate) inner: TopicSender,
    pub ready_fut: Mutex<Option<BoxFuture<'static, ()>>>,
}

#[uniffi::export]
impl Sender {
    /// Broadcast a message to all peers subscribing to the same topic.
    pub async fn send(&self, message: &Message) -> Result<MessageMeta, RhioError> {
        let message = rhio::messages::Message::from(message.clone());
        let meta = self.inner.send(message).await?;
        Ok(MessageMeta(meta))
    }

    /// Broadcast a blob announcement message to all peers subscribing to the same topic.
    pub async fn announce_blob(&self, hash: Hash) -> Result<MessageMeta, RhioError> {
        let meta = self.inner.announce_blob(hash.into()).await?;
        Ok(MessageMeta(meta))
    }

    /// Wait for another peer to be subscribed to this topic.
    pub async fn ready(&self) {
        let fut = self.ready_fut.lock().unwrap().take();
        if let Some(fut) = fut {
            fut.await
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use p2panda_core::PublicKey;
    use rhio::config::KnownNode;
    use tokio::sync::mpsc;

    use crate::config::Config;
    use crate::error::CallbackError;
    use crate::messages::Message;

    use super::*;

    struct Cb {
        channel: mpsc::Sender<(Arc<Message>, Arc<MessageMeta>)>,
    }

    #[async_trait::async_trait]
    impl GossipMessageCallback for Cb {
        async fn on_message(
            &self,
            message: Arc<Message>,
            meta: Arc<MessageMeta>,
        ) -> Result<(), CallbackError> {
            self.channel
                .send((message, meta))
                .await
                .expect("could not send on callback channel");
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_gossip_basic() {
        let config0 = Config::default();
        let n0 = Node::spawn(&config0).await.unwrap();
        let n0_id: PublicKey = n0.id().into();
        let n0_addresses = n0
            .direct_addresses()
            .await
            .expect("has direct addresses")
            .into_iter()
            .map(Into::into)
            .collect();

        let mut config1 = Config::default();
        config1.inner.node.bind_port = 2023;
        config1.inner.node.known_nodes = vec![KnownNode {
            public_key: n0_id,
            direct_addresses: n0_addresses,
        }];

        let n1 = Node::spawn(&config1).await.unwrap();

        tokio::time::sleep(Duration::from_secs(2)).await;

        let topic = TopicId::from_str("test");

        let (sender0, _receiver0) = mpsc::channel(8);
        let cb0 = Cb { channel: sender0 };

        let (sender1, mut receiver1) = mpsc::channel(8);
        let cb1 = Cb { channel: sender1 };

        let sender0 = n0.subscribe(&topic, Arc::new(cb0)).await.unwrap();
        let sender1 = n1.subscribe(&topic, Arc::new(cb1)).await.unwrap();

        sender0.ready().await;
        sender1.ready().await;

        // Send message on n0
        let msg_content = b"hello";
        sender0
            .send(&Message::Application(msg_content.to_vec()))
            .await
            .unwrap();

        // Receive on n1
        let recv_fut = async {
            let Some(event) = receiver1.recv().await else {
                panic!("receiver stream closed before receiving gossip message");
            };
            let (message, meta) = &event;
            assert_eq!(message.as_application(), msg_content);
            assert_eq!(meta.delivered_from().to_string(), n0_id.to_string());
        };

        tokio::time::timeout(std::time::Duration::from_secs(10), recv_fut)
            .await
            .expect("timeout reached and no gossip message received");
    }
}
