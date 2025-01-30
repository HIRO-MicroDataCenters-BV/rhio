use anyhow::Result;
use async_trait::async_trait;

use crate::status::HealthStatus;

pub const HTTP_HEALTH_ROUTE: &str = "/health";
pub const HTTP_METRICS_ROUTE: &str = "/metrics";

#[async_trait]
pub trait RhioApi: Send + Sync {
    async fn health(&self) -> Result<HealthStatus>;

    async fn metrics(&self) -> Result<String>;
}
