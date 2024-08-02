use std::path::PathBuf;
use std::sync::Arc;

use p2panda_core::Hash; 
use rhio::config::{Config as RhioConfig};
use rhio::private_key::generate_ephemeral_private_key;
use rhio::topic_id::TopicId;
use rhio::Node as RhioNode;
use tracing::warn;
use uniffi;

use crate::config::Config;
use crate::error::RhioError;
use crate::messages::{GossipMessageCallback, MessageMeta, Sender};

#[derive(uniffi::Object)]
pub struct Node {
    pub inner: RhioNode<Vec<u8>>,
}

#[uniffi::export]
impl Node {
    #[uniffi::constructor(async_runtime = "tokio")]
    pub async fn spawn(config: &Config) -> Result<Node, RhioError> {
        let private_key = generate_ephemeral_private_key();
        let config: RhioConfig = config.clone().try_into()?;
        let rhio_node = RhioNode::spawn(config, private_key).await?;
        Ok(Node { inner: rhio_node })
    }

    #[uniffi::method]
    pub fn id(&self) -> String {
        self.inner.id().to_hex()
    }

    pub async fn import_blob(&self, path: String) -> Result<(), RhioError> {
        let path = PathBuf::from(&path);
        self.inner.import_blob(path).await?;
        Ok(())
    }

    pub async fn export_blob(&self, hash: String, path: String) -> Result<(), RhioError> {
        let hash: Hash = hash.parse().map_err(anyhow::Error::from)?;
        let path = PathBuf::from(&path);
        self.inner.export_blob(hash, path).await?;
        Ok(())
    }

    #[uniffi::method(async_runtime = "tokio")]
    pub async fn subscribe(
        &self,
        topic: &str,
        cb: Arc<dyn GossipMessageCallback>,
    ) -> Result<Sender, RhioError> {
        let topic_id = TopicId::new_from_str(topic);

        let (topic_tx, mut topic_rx, ready) = self.inner.subscribe(topic_id).await?;

        ready.await;

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

        Ok(Sender(topic_tx))
    }
}

// @TODO: need to form a connection between the test nodes
//
// #[cfg(test)]
// mod tests {
//     use std::time::Duration;
//
//     use tokio::sync::mpsc;
//
//     use crate::node::Config;
//
//     use super::*;
//
//     #[tokio::test]
//     async fn test_gossip_basic() {
//         let config = Config::default();
//         let n0 = Node::spawn(&config).await.unwrap();
//         let n1 = Node::spawn(&config).await.unwrap();
//
//         let n0_id = n0.id();
//
//         struct Cb {
//             channel: mpsc::Sender<Arc<MessageWithMeta>>,
//         }
//         #[async_trait::async_trait]
//         impl GossipMessageCallback for Cb {
//             async fn on_message(&self, message: Arc<MessageWithMeta>) -> Result<(), CallbackError> {
//                 println!("<< {:?}", message);
//                 self.channel.send(message).await.unwrap();
//                 Ok(())
//             }
//         }
//
//         let topic = "test";
//
//         let (sender0, _receiver0) = mpsc::channel(8);
//         let cb0 = Cb { channel: sender0 };
//
//         let (sender1, mut receiver1) = mpsc::channel(8);
//         let cb1 = Cb { channel: sender1 };
//
//         let sender0 = n0.subscribe(topic, Arc::new(cb0)).await.unwrap();
//         let _sender1 = n1.subscribe(topic, Arc::new(cb1)).await.unwrap();
//
//         // @TODO: connect nodes
//
//         // Send message on n0
//         println!("sending message");
//         let msg_content = b"hello";
//         sender0
//             .send(&Message(InnerMessage::Application(msg_content.to_vec())))
//             .await
//             .unwrap();
//
//         // Receive on n1
//         let recv_fut = async {
//             loop {
//                 let Some(event) = receiver1.recv().await else {
//                     panic!("receiver stream closed before receiving gossip message");
//                 };
//                 println!("event: {:?}", event);
//                 let MessageWithMeta {
//                     ref content,
//                     ref meta,
//                 } = &*event;
//
//                 if let InnerMessage::Application(bytes) = content {
//                     assert_eq!(bytes, msg_content);
//                 };
//
//                 assert_eq!(meta.delivered_from.to_string(), n0_id.to_string());
//             }
//         };
//         tokio::time::timeout(std::time::Duration::from_secs(10), recv_fut)
//             .await
//             .expect("timeout reached and no gossip message received");
//     }
// }
