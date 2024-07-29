use std::time::Duration;

use anyhow::Result;
use p2panda_core::PrivateKey;
use rhio::config::Config;
use rhio::messages::Message;
use rhio::node::Node;
use rhio::topic_id::TopicId;

/// The only message type in our chat app
type ChatMessage = String;

#[tokio::main]
async fn main() -> Result<()> {
    let private_key = PrivateKey::new();
    let mut config = Config::default();

    // Add a topic to the node configuration
    let chat_topic_id = TopicId::from_str("my_chat");
    config.topics.push(chat_topic_id);

    // Spawn the node
    let mut node: Node<ChatMessage> = Node::spawn(config.clone(), private_key.clone()).await?;

    println!("Peer Id: {}", private_key.public_key().to_hex());

    println!("joining gossip overlay ..");
    let _ = node.ready().await;
    println!("gossip overlay joined!");

    // Get channels for sending and receiving messages on the chat topic
    let (chat_tx, mut chat_rx) = node.topic(chat_topic_id).await?;

    // Listen for arriving messages
    tokio::spawn(async move {
        while let Ok(Message::Application(message)) = chat_rx.recv().await {
            println!(
                "{} on {}",
                message,
                chat_topic_id.to_string()
            )
        }
    });

    // Send "hello" to all peers
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            chat_tx
                .send(Message::Application(String::from("hello")))
                .await
                .expect("can send message on channel");
        }
    });

    tokio::time::sleep(Duration::from_secs(20)).await;
    node.shutdown().await?;

    Ok(())
}
