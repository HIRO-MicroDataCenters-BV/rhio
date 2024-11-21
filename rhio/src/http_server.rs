use axum::{
    http::{Response, StatusCode},
    response::{Html, IntoResponse},
    routing::get,
    Router,
};
use tracing::debug;

use crate::config::DEFAULT_HTTP_BIND_PORT;

pub async fn run(bind_port: u16) -> anyhow::Result<()> {
    // build our application with a route
    let app = Router::new().route("/health", get(handler_200));

    // run it
    let listener = tokio::net::TcpListener::bind(format!("localhost:{bind_port}")).await?;
    debug!(
        "HTTP health endpoint listening on {}",
        listener.local_addr()?
    );
    axum::serve(listener, app).await?;
    Ok(())
}

async fn handler_200() -> impl IntoResponse {
    (StatusCode::OK, "rhio service active")
}
