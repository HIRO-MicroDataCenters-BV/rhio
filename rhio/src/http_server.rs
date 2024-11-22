use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;
use tracing::debug;

pub const HTTP_HEALTH_ROUTE: &str = "/health";

pub async fn run(bind_port: u16) -> anyhow::Result<()> {
    let app = Router::new().route(HTTP_HEALTH_ROUTE, get(health));
    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{bind_port}")).await?;
    debug!(
        "HTTP health endpoint listening on {}",
        listener.local_addr()?
    );
    axum::serve(listener, app).await?;
    Ok(())
}

async fn health() -> impl IntoResponse {
    (StatusCode::OK, "rhio service active")
}
