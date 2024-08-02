use std::path::PathBuf;
use std::sync::Arc;

use rhio::config::{parse_node_addr, parse_url, Config as RhioConfig, NodeAddr};
use rhio::private_key::generate_ephemeral_private_key;
use rhio::topic_id::TopicId;
use rhio::Node as RhioNode;
use tracing::warn;
use uniffi;

use crate::error::RhioError;
use crate::messages::{GossipMessageCallback, MessageMeta, Sender};

#[derive(Default, Clone, uniffi::Record)]
pub struct Config {
    #[uniffi(default = None)]
    pub blobs_path: Option<String>,
    #[uniffi(default = 2024)]
    pub bind_port: u16,
    #[uniffi(default = None)]
    pub private_key: Option<String>,
    #[uniffi(default = [])]
    pub direct_node_addresses: Vec<String>,
    #[uniffi(default = [])]
    pub relay_addresses: Vec<String>,
}

impl TryInto<RhioConfig> for Config {
    type Error = RhioError;

    fn try_into(self) -> Result<RhioConfig, Self::Error> {
        let mut config = RhioConfig::default();
        if let Some(path) = self.blobs_path {
            config.blobs_path = Some(PathBuf::from(&path));
        };

        config.network_config.bind_port = self.bind_port;

        if let Some(path) = self.private_key {
            config.network_config.private_key = Some(PathBuf::from(path));
        }

        config.network_config.direct_node_addresses = self
            .direct_node_addresses
            .iter()
            .map(|addr| parse_node_addr(addr))
            .collect::<Result<Vec<NodeAddr>, _>>()?;

        config.network_config.relay_addresses = self
            .relay_addresses
            .iter()
            .map(|url_str| parse_url(url_str))
            .collect::<Result<_, _>>()?;
        Ok(config)
    }
}

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
