use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
};

use anyhow::{Context, Result};
use axum::Router;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, extract::State};
use tokio::net::TcpListener;
use tracing::debug;

use crate::{
    api::{HTTP_HEALTH_ROUTE, HTTP_METRICS_ROUTE, RhioApi},
    status::HealthStatus,
};

/// `RhioHTTPServer` is a struct that represents an HTTP server for the Rhio application.
/// It is responsible for serving health and metrics endpoints over HTTP.
///
/// # Fields
/// - `port`: The port on which the server will listen for incoming HTTP requests.
/// - `api`: An `Arc` to a trait object implementing `RhioApi`, which provides the health and metrics functionality.
///
/// # Methods
/// - `new(port: u16, api: Arc<dyn RhioApi>) -> RhioHTTPServer`
///   - Creates a new instance of `RhioHTTPServer` with the specified port and API implementation.
/// - `run(&self) -> Result<()>`
///   - Asynchronously runs the HTTP server, binding to the specified port and serving the health and metrics endpoints.
///   - Returns a `Result` indicating success or failure of the server operation.
///
pub struct RhioHTTPServer {
    port: u16,
    api: Arc<dyn RhioApi>,
}

impl RhioHTTPServer {
    pub fn new(port: u16, api: Arc<dyn RhioApi>) -> RhioHTTPServer {
        RhioHTTPServer { port, api }
    }

    pub async fn run(&self) -> Result<()> {
        let listener = TcpListener::bind(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::UNSPECIFIED),
            self.port,
        ))
        .await
        .context("TCP Listener binding")?;
        debug!(
            "HTTP health and metrics endpoint listening on {}",
            listener.local_addr()?
        );
        let state = ServerState {
            api: self.api.clone(),
        };

        let app = Router::new()
            .route(HTTP_HEALTH_ROUTE, get(health))
            .route(HTTP_METRICS_ROUTE, get(metrics))
            .with_state(state);
        axum::serve(listener, app)
            .await
            .context("HTTP metrics and health serving")?;

        Ok(())
    }
}

async fn health(State(state): State<ServerState>) -> impl IntoResponse {
    state
        .api
        .health()
        .await
        .map(|result| (StatusCode::OK, Json(result)))
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(Into::<HealthStatus>::into(e)),
            )
        })
}

async fn metrics(State(state): State<ServerState>) -> impl IntoResponse {
    state
        .api
        .metrics()
        .await
        .map(|result| (StatusCode::OK, result))
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("{:?}", e)))
}

#[derive(Clone)]
pub struct ServerState {
    pub api: Arc<dyn RhioApi>,
}
