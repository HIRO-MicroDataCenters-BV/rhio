use std::sync::Arc;

use rhio::messages::{Message as InnerMessage, MessageMeta as InnerMessageMeta};
use rhio::node::TopicSender;
use rhio::topic_id::TopicId;
use tracing::warn;

use crate::error::{CallbackError, RhioError};
use crate::node::Node;

#[derive(Debug, Clone, uniffi::Object)]
pub struct MessageWithMeta {
    content: InnerMessage<Vec<u8>>,
    meta: InnerMessageMeta,
}

#[uniffi::export]
impl MessageWithMeta {
    #[uniffi::method]
    pub fn content(&self) -> Vec<u8> {
        match &self.content {
            InnerMessage::Application(bytes) => bytes.clone(),
            _ => unimplemented!(),
        }
    }

    #[uniffi::method]
    pub fn delivered_from(&self) -> String {
        self.meta.delivered_from.to_string()
    }
}

#[derive(Debug, Clone, uniffi::Object)]
pub struct Message(InnerMessage<Vec<u8>>);

#[uniffi::export]
impl Message {
    #[uniffi::constructor]
    pub fn new(content: &[u8]) -> Self {
        Message(InnerMessage::Application(content.to_vec()))
    }
}

#[derive(Debug, Clone, uniffi::Object)]
pub struct MessageMeta(InnerMessageMeta);

#[uniffi::export(with_foreign)]
#[async_trait::async_trait]
pub trait GossipMessageCallback: Send + Sync + 'static {
    async fn on_message(&self, msg: Arc<MessageWithMeta>) -> Result<(), CallbackError>;
}

#[uniffi::export]
impl Node {
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
                    .on_message(Arc::new(MessageWithMeta {
                        content: message,
                        meta,
                    }))
                    .await
                {
                    warn!("cb error, gossip: {:?}", err);
                }
            }
        });

        Ok(Sender(topic_tx))
    }
}

#[derive(uniffi::Object)]
pub struct Sender(TopicSender<Vec<u8>>);

#[uniffi::export]
impl Sender {
    pub async fn send(&self, message: &Message) -> Result<MessageMeta, RhioError> {
        let meta = self.0.send(message.clone().0).await?;
        Ok(MessageMeta(meta))
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
