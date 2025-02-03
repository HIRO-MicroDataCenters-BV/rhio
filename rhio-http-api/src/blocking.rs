use std::sync::Arc;

use crate::{api::RhioApi, status::HealthStatus};
use anyhow::Result;
use tokio::runtime::Runtime;

/// A blocking client for interacting with the Rhio HTTP API.
///
/// The `BlockingClient` struct provides a synchronous interface to the asynchronous
/// `RhioApi` by using a Tokio runtime to block on asynchronous calls. This allows
/// users to interact with the API in a blocking manner, which can be useful in
/// contexts where asynchronous code is not suitable.
///
/// # Type Parameters
///
/// * `A` - A type that implements the `RhioApi` trait.
///
/// # Fields
///
/// * `inner` - The inner API client that implements the `RhioApi` trait.
/// * `runtime` - An `Arc` wrapped Tokio runtime used to block on asynchronous calls.
///
pub struct BlockingClient<A>
where
    A: RhioApi,
{
    inner: A,
    runtime: Arc<Runtime>,
}

impl<A> BlockingClient<A>
where
    A: RhioApi,
{
    pub fn new(inner: A, runtime: Arc<Runtime>) -> Self {
        Self { inner, runtime }
    }

    pub fn health(&self) -> Result<HealthStatus> {
        self.runtime.block_on(async { self.inner.health().await })
    }

    pub fn metrics(&self) -> Result<String> {
        self.runtime.block_on(async { self.inner.metrics().await })
    }
}
