use rhio::config::Config;
use rhio::private_key::generate_ephemeral_private_key;
use rhio::Node as RhioNode;
use uniffi;

uniffi::setup_scaffolding!();

#[derive(Debug, thiserror::Error, uniffi::Object)]
#[error("{e:?}")]
#[uniffi::export(Debug)]
pub struct RhioError {
    e: anyhow::Error,
}

#[uniffi::export]
impl RhioError {
    pub fn message(&self) -> String {
        self.to_string()
    }
}

impl From<anyhow::Error> for RhioError {
    fn from(e: anyhow::Error) -> Self {
        Self { e }
    }
}

#[derive(uniffi::Object)]
pub struct Node {
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
pub async fn run() -> Result<Node, RhioError> {
    let config = Config::default();
    let private_key = generate_ephemeral_private_key();
    let rhio_node = RhioNode::spawn(config.network_config, private_key)
        .await?;
    Ok(Node { inner: rhio_node })
}
