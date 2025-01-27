use reqwest::Error;
use rhio_config::status::HealthStatus;

use crate::api::service::RhioService;

pub async fn fetch_status(rhio: &RhioService) -> Result<HealthStatus, Error> {
    let url = format!(
        "http://{}.default.svc.cluster.local:8080/health",
        &rhio.metadata.name.clone().unwrap()
    );
    // let url = format!("http://localhost:8080/health");
    let response = reqwest::get(url).await?.json::<HealthStatus>().await?;
    Ok(response)
}
