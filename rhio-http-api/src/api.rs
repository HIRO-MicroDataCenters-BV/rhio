use anyhow::Result;
use async_trait::async_trait;

use crate::status::HealthStatus;

pub const HTTP_HEALTH_ROUTE: &str = "/health";
pub const HTTP_METRICS_ROUTE: &str = "/metrics";

/// Trait representing the Rhio Http API, which provides methods for checking the health status
/// and retrieving metrics of the service.
///
/// # Methods
///
/// * `health` - Asynchronously checks the health status of the service and returns a `HealthStatus` result.
/// * `metrics` - Asynchronously retrieves metrics of the service and returns them as a `String` result.
///
#[async_trait]
pub trait RhioApi: Send + Sync {
    async fn health(&self) -> Result<HealthStatus>;

    async fn metrics(&self) -> Result<String>;
}
