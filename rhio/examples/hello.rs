use std::time::Duration;

use anyhow::Result;
use p2panda_core::PrivateKey;
use p2panda_net::ToBytes;
use rhio::config::Config;
use rhio::messages::{Message, MessageMeta};
use rhio::node::Node;
use rhio::ticket::Ticket;
use rhio::topic_id::TopicId;

/// The only message type in our chat app
type ChatMessage = Vec<u8>;

#[tokio::main]
async fn main() -> Result<()> {
    let chat_topic_id = TopicId::new_from_str("rhio/hello_world");
    let private_key = PrivateKey::new();
    let config = Config::default();
    let relay = config.network_config.relay.clone();

    // Spawn the node
    let node: Node<ChatMessage> =
        Node::spawn(config.clone(), private_key.clone()).await?;

    if let Some(addresses) = node.direct_addresses().await {
        match &relay {
            Some(url) => {
                println!("‣ relay url: {}", url);
            }
            None => {
                println!("! you haven't specified a relay address for your node");
                println!("other peers might not be able to connect to you without it.");
            }
        }
        let ticket = Ticket::new(node.id(), addresses, relay);
        println!("‣ connection ticket: {}", ticket);
    } else {
        println!("‣ node public key: {}", node.id());
    }

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
