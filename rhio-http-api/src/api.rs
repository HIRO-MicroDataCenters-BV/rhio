use anyhow::Result;
use async_trait::async_trait;

use crate::status::HealthStatus;

#[async_trait]
pub trait RhioApi: Send + Sync {
    async fn health(&self) -> Result<HealthStatus>;

    async fn metrics(&self) -> Result<String>;
}
