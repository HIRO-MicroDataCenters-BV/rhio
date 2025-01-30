use crate::{
    api::RhioApi,
    api::{HTTP_HEALTH_ROUTE, HTTP_METRICS_ROUTE},
    status::HealthStatus,
};
use anyhow::{Context, Result};
use async_trait::async_trait;

pub struct RhioApiClient {
    endpoint: String,
}

impl RhioApiClient {
    pub fn new(endpoint: String) -> RhioApiClient {
        RhioApiClient { endpoint }
    }
}

#[async_trait]
impl RhioApi for RhioApiClient {
    async fn health(&self) -> Result<HealthStatus> {
        let url = format!("{}{}", self.endpoint, HTTP_HEALTH_ROUTE);
        let response = reqwest::get(url)
            .await
            .context("health request")?
            .json::<HealthStatus>()
            .await
            .context("health response deserialization")?;
        Ok(response)
    }

    async fn metrics(&self) -> Result<String> {
        let url = format!("{}{}", self.endpoint, HTTP_METRICS_ROUTE);
        let response = reqwest::get(url)
            .await
            .context("metrics request")?
            .text()
            .await
            .context("metrics response deserialization")?;
        Ok(response)
    }
}
