use std::sync::Arc;

use crate::{api::RhioApi, status::HealthStatus};
use anyhow::Result;
use tokio::runtime::Runtime;

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
