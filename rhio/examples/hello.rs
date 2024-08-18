use std::time::Duration;

use anyhow::Result;
use p2panda_core::PrivateKey;
use p2panda_net::ToBytes;
use rhio::config::Config;
use rhio::messages::{Message, MessageMeta};
use rhio::node::Node;
use rhio::topic_id::TopicId;
use tokio_util::task::LocalPoolHandle;

/// The only message type in our chat app
type ChatMessage = Vec<u8>;

#[tokio::main]
async fn main() -> Result<()> {
    let pool_handle = LocalPoolHandle::new(num_cpus::get());

    let chat_topic_id = TopicId::new_from_str("rhio/hello_world");
    let private_key = PrivateKey::new();
    let config = Config::default();

    // Spawn the node
    let node: Node<ChatMessage> =
        Node::spawn(config.clone(), private_key.clone(), pool_handle).await?;

    println!("Peer Id: {}", private_key.public_key().to_hex());

    println!("joining gossip overlay ..");
    let (chat_tx, mut chat_rx, ready) = node.subscribe(chat_topic_id).await?;
    ready.await;
    println!("gossip overlay joined!");

    // Listen for arriving messages
    tokio::spawn(async move {
        while let Ok((
            Message::Application(bytes),
            MessageMeta {
                delivered_from,
                received_at,
                ..
            },
        )) = chat_rx.recv().await
        {
            println!(
                "{} from {} on topic {} at {}",
                String::from_utf8(bytes).expect("valid UTF-8 string bytes"),
                delivered_from,
                chat_topic_id.to_string(),
                received_at
            )
        }
    });

    // Send "hello" to all peers
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            chat_tx
                .send(Message::Application("Hello from Rust!".to_bytes()))
                .await
                .expect("can send message on channel");
        }
    });

    tokio::time::sleep(Duration::from_secs(20)).await;
    node.shutdown().await?;

    Ok(())
}