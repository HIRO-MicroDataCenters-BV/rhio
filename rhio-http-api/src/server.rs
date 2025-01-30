use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
};

use anyhow::{Context, Result};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;
use axum::{extract::State, Json};
use tokio::net::TcpListener;
use tracing::debug;

use crate::{
    api::{RhioApi, HTTP_HEALTH_ROUTE, HTTP_METRICS_ROUTE},
    status::HealthStatus,
};

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
