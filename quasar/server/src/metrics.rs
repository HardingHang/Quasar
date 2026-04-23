//! Metrics endpoint and HTTP request tracking middleware.

use axum::{
    extract::Request, http::StatusCode, middleware::Next, response::Response, routing::get, Json,
    Router,
};
use quasar_core::MetricsState;
use serde::Serialize;
use std::time::Instant;
use tracing::info;

/// Prometheus metrics scrape endpoint.
pub async fn metrics_handler(
    axum::Extension(state): axum::Extension<MetricsState>,
) -> (StatusCode, String) {
    (StatusCode::OK, state.registry.render())
}

/// Simple health check for metrics endpoint.
#[derive(Serialize)]
pub struct MetricsHealthResponse {
    pub status: String,
}

pub async fn metrics_health() -> Json<MetricsHealthResponse> {
    Json(MetricsHealthResponse {
        status: "ok".to_string(),
    })
}

/// Create routes for metrics endpoints.
pub fn routes<S>() -> Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    Router::new()
        .route("/metrics", get(metrics_handler))
        .route("/metrics/health", get(metrics_health))
}

/// HTTP request tracking middleware.
pub async fn track_requests(
    axum::Extension(state): axum::Extension<MetricsState>,
    req: Request,
    next: Next,
) -> Response {
    let start = Instant::now();
    let method = req.method().clone();
    let path = req.uri().path().to_string();

    let response = next.run(req).await;

    let status = response.status().as_u16();
    let label_path = normalize_path(&path);

    state
        .registry
        .record_http_request(method.as_str(), &label_path, status);

    // Log slow requests (> 1s for MVP threshold)
    let duration = start.elapsed();
    if duration.as_secs_f64() > 1.0 {
        tracing::warn!(
            method = %method,
            path = %path,
            status = status,
            duration_ms = %duration.as_millis(),
            "slow request"
        );
    }

    response
}

/// Request ID injection middleware.
pub async fn request_id(req: Request, next: Next) -> Response {
    let request_id = req
        .headers()
        .get("x-request-id")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

    let mut response = next.run(req).await;
    if let Ok(val) = request_id.parse() {
        response.headers_mut().insert("x-request-id", val);
    }
    response
}

/// Normalize dynamic path segments for metrics labels to prevent cardinality explosion.
fn normalize_path(path: &str) -> String {
    if path.starts_with("/iceberg/v1/namespaces/") {
        if let Some(rest) = path.strip_prefix("/iceberg/v1/namespaces/") {
            if let Some((_, tail)) = rest.split_once("/tables/") {
                let table_part = if tail.contains('/') {
                    let parts: Vec<&str> = tail.splitn(2, '/').collect();
                    format!("{{table}}/{}", parts[1])
                } else {
                    "{table}".to_string()
                };
                return format!("/iceberg/v1/namespaces/{{ns}}/tables/{}", table_part);
            }
            return "/iceberg/v1/namespaces/{ns}".to_string();
        }
    }
    if path.starts_with("/lance/v1/namespace/") {
        return "/lance/v1/namespace/{id}/{action}".to_string();
    }
    if path.starts_with("/lance/v1/table/") {
        return "/lance/v1/table/{id}/{action}".to_string();
    }
    path.to_string()
}

/// Initialize default application-level metrics.
pub fn init_app_metrics() {
    info!("application metrics initialized");
}
