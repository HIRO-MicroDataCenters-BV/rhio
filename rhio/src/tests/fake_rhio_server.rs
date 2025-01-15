use crate::{config::Config, context::Context, context_builder::ContextBuilder};
use anyhow::Result;
use p2panda_core::PrivateKey;

/// A fake server for testing purposes in the Rhio project.
///
/// The `FakeRhioServer` struct provides methods to start and discard a fake server instance.
///
/// # Fields
///
/// * `context` - The context in which the server operates.
///
/// # Methods
///
/// * `try_start` - Attempts to start the fake server with the given configuration and private key.
/// * `discard` - Shuts down the fake server and cleans up resources.
pub struct FakeRhioServer {
    context: Context,
}

impl FakeRhioServer {
    pub fn try_start(config: Config, private_key: PrivateKey) -> Result<Self> {
        let builder = ContextBuilder::new(config, private_key);
        let context = builder.try_build_and_start()?;
        context.configure()?;
        context.log_configuration();
        Ok(FakeRhioServer { context })
    }

    pub fn discard(self) -> Result<()> {
        self.context.shutdown()
    }
}
