use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use anyhow::{Context, Result};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;
use axum_prometheus::metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use std::future::ready;
use tokio::net::TcpListener;
use tracing::debug;

pub const HTTP_HEALTH_ROUTE: &str = "/health";

pub const HTTP_METRICS_ROUTE: &str = "/metrics";

pub async fn run_http_server(bind_port: u16) -> Result<()> {
    let recorder_handle = setup_metrics_recorder()?;
    let listener = TcpListener::bind(SocketAddr::new(
        IpAddr::V4(Ipv4Addr::UNSPECIFIED),
        bind_port,
    ))
    .await
    .context("TCP Listener binding")?;
    debug!(
        "HTTP health and metrics endpoint listening on {}",
        listener.local_addr()?
    );

    let app = Router::new().route(HTTP_HEALTH_ROUTE, get(health)).route(
        HTTP_METRICS_ROUTE,
        get(move || ready(recorder_handle.render())),
    );
    axum::serve(listener, app)
        .await
        .context("HTTP metrics and health serving")?;
    Ok(())
}

async fn health() -> impl IntoResponse {
    (StatusCode::OK, "rhio service active")
}

fn setup_metrics_recorder() -> Result<PrometheusHandle> {
    let builder = PrometheusBuilder::new()
        .install_recorder()
        .context("Installing global prometheus recorder")?;
    Ok(builder)
}
