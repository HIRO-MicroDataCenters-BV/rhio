use rhio::config::Config;
use rhio::private_key::generate_ephemeral_private_key;
use rhio::Node as RhioNode;
use uniffi;

uniffi::setup_scaffolding!();

#[derive(uniffi::Object)]
struct Node {
    pub inner: RhioNode,
}

#[uniffi::export]
impl Node {
    #[uniffi::method]
    pub fn id(&self) -> String {
        self.inner.id().to_hex()
    }
}

#[uniffi::export(async_runtime = "tokio")]
pub async fn run() -> Node {
    let config = Config::default();
    let private_key = generate_ephemeral_private_key();
    let rhio_node = RhioNode::spawn(config.network_config, private_key)
        .await
        .expect("Node can start");
    Node { inner: rhio_node }
}
