use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use anyhow::{Context, Result};
use axum::extract::State;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;
use axum_prometheus::metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use rhio_config::configuration::Config;
use std::future::ready;
use tokio::net::TcpListener;
use tracing::debug;

use crate::http::status::status_handler;

pub const HTTP_HEALTH_ROUTE: &str = "/health";

pub const HTTP_METRICS_ROUTE: &str = "/metrics";

#[derive(Default, Clone)]
pub struct ServerState {
    pub config: Config,
}

pub async fn run_http_server(bind_port: u16, config: Config) -> Result<()> {
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
    let state = ServerState { config };
    let app = Router::new()
        .route(HTTP_HEALTH_ROUTE, get(health))
        .route(
            HTTP_METRICS_ROUTE,
            get(move || ready(recorder_handle.render())),
        )
        .with_state(state);
    axum::serve(listener, app)
        .await
        .context("HTTP metrics and health serving")?;
    Ok(())
}

async fn health(State(state): State<ServerState>) -> impl IntoResponse {
    let result = status_handler(&state.config).await;
    (StatusCode::OK, result)
}

fn setup_metrics_recorder() -> Result<PrometheusHandle> {
    let builder = PrometheusBuilder::new()
        .install_recorder()
        .context("Installing global prometheus recorder")?;
    Ok(builder)
}
