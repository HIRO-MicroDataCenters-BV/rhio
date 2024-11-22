use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use anyhow::Result;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;
use tokio::net::TcpListener;
use tracing::debug;

pub const HTTP_HEALTH_ROUTE: &str = "/health";

pub async fn run_http_server(bind_port: u16) -> Result<()> {
    let listener = TcpListener::bind(SocketAddr::new(
        IpAddr::V4(Ipv4Addr::UNSPECIFIED),
        bind_port,
    ))
    .await?;
    debug!(
        "HTTP health endpoint listening on {}",
        listener.local_addr()?
    );
    let app = Router::new().route(HTTP_HEALTH_ROUTE, get(health));
    axum::serve(listener, app).await?;
    Ok(())
}

async fn health() -> impl IntoResponse {
    (StatusCode::OK, "rhio service active")
}
