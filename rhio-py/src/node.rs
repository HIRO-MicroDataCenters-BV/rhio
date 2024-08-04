use futures::future::BoxFuture;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use futures::FutureExt;
use p2panda_core::Hash;
use rhio::config::Config as RhioConfig;
use rhio::node::TopicSender;
use rhio::private_key::generate_ephemeral_private_key;
use rhio::topic_id::TopicId;
use rhio::Node as RhioNode;
use tracing::warn;
use uniffi;

use crate::config::Config;
use crate::error::{CallbackError, RhioError};
use crate::messages::{Message, MessageMeta};

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
    pub async fn spawn(config: &Config) -> Result<Node, RhioError> {
        let private_key = generate_ephemeral_private_key();
        let config: RhioConfig = config.clone().try_into()?;
        let rhio_node = RhioNode::spawn(config, private_key).await?;
        Ok(Node { inner: rhio_node })
    }

    /// Returns the PublicKey of this node which is used as it's unique network id.
    ///
    /// This ID is the unique addressing information of this node and other peers must know it to
    /// be able to connect to this node.
    #[uniffi::method]
    pub fn id(&self) -> String {
        self.inner.id().to_hex()
    }

    /// Returns the direct addresses of this Node.
    ///
    /// The direct addresses of the Node are those that could be used by other nodes
    /// to establish direct connectivity, depending on the network situation. The yielded lists of
    /// direct addresses contain both the locally-bound addresses and the Node's publicly
    /// reachable addresses discovered through mechanisms such as STUN and port mapping. Hence
    /// usually only a subset of these will be applicable to a certain remote node.
    pub async fn direct_addresses(&self) -> Option<Vec<String>> {
        match self.inner.direct_addresses().await {
            Some(addrs) => Some(addrs.iter().map(|addr| addr.to_string()).collect()),
            None => None,
        }
    }

    /// Import a blob from the filesystem.
    ///
    /// This method moves a blob into dedicated blob store and makes it available on the network
    /// identified by it's Blake3 hash.
    pub async fn import_blob(&self, path: String) -> Result<String, RhioError> {
        let path = PathBuf::from(&path);
        let hash = self.inner.import_blob(path).await?;
        Ok(hash.to_string())
    }

    /// Export a blob to the filesystem.
    ///
    /// Copies an existing blob from the blob store to a location on the filesystem.
    pub async fn export_blob(&self, hash: String, path: String) -> Result<(), RhioError> {
        let hash: Hash = hash.parse().map_err(anyhow::Error::from)?;
        let path = PathBuf::from(&path);
        self.inner.export_blob(hash, path).await?;
        Ok(())
    }

    /// Download a blob from the network.
    ///
    /// Attempt to download a blob from peers on the network and place it into the nodes blob store.
    pub async fn download_blob(&self, hash: String) -> Result<(), RhioError> {
        let hash: Hash = hash.parse().map_err(anyhow::Error::from)?;
        self.inner.download_blob(hash).await?;
        Ok(())
    }

    /// Subscribe to a gossip topic.
    ///
    /// Accepts a callback method which should be used to handle messages arriving on this topic.
    /// Returns a sender which can then be used to broadcast events to all peers also subscribed
    /// to this topic. The sender can be awaited using it's `ready()` method which only resolves
    /// when at least one other peers subscribes to the same topic.
    #[uniffi::method(async_runtime = "tokio")]
    pub async fn subscribe(
        &self,
        topic: &str,
        cb: Arc<dyn GossipMessageCallback>,
    ) -> Result<Sender, RhioError> {
        let topic_id = TopicId::new_from_str(topic);

        let (topic_tx, mut topic_rx, ready) = self.inner.subscribe(topic_id).await?;

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

/// Callback used to handle all incomming messages on a particular topic.
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

    /// Wait for another peer to be subscribed to this topic.
    pub async fn ready(&self) {
        let fut = self.ready_fut.lock().unwrap().take();
        match fut {
            Some(fut) => fut.await,
            None => (),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tokio::sync::mpsc;

    use crate::messages::Message;
    use crate::{error::CallbackError, node::Config};

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
            println!("<< {:?}", message);
            self.channel.send((message, meta)).await.unwrap();
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_gossip_basic() {
        let config0 = Config::default();
        let n0 = Node::spawn(&config0).await.unwrap();
        let n0_id = n0.id();

        let n0_addr = format!("{}|127.0.0.1:2024", n0_id);
        let config1 = Config {
            bind_port: 2023,
            direct_node_addresses: vec![n0_addr],
            ..Default::default()
        };
        let n1 = Node::spawn(&config1).await.unwrap();

        tokio::time::sleep(Duration::from_secs(2)).await;

        let topic = "test";

        let (sender0, _receiver0) = mpsc::channel(8);
        let cb0 = Cb { channel: sender0 };

        let (sender1, mut receiver1) = mpsc::channel(8);
        let cb1 = Cb { channel: sender1 };

        let sender0 = n0.subscribe(topic, Arc::new(cb0)).await.unwrap();
        let sender1 = n1.subscribe(topic, Arc::new(cb1)).await.unwrap();

        sender0.ready().await;
        sender1.ready().await;

        // Send message on n0
        println!("sending message");
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
