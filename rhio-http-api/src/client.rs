use crate::{
    api::RhioApi,
    server::{HTTP_HEALTH_ROUTE, HTTP_METRICS_ROUTE},
    status::HealthStatus,
};
use anyhow::Result;
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
        let response = reqwest::get(url).await?.json::<HealthStatus>().await?;
        Ok(response)
    }

    async fn metrics(&self) -> Result<String> {
        let url = format!("{}{}", self.endpoint, HTTP_METRICS_ROUTE);
        let response = reqwest::get(url).await?.json::<String>().await?;
        Ok(response)
    }
}
