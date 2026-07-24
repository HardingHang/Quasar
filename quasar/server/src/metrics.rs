//! Metrics collection, Prometheus scrape endpoint, and HTTP request
//! tracking middleware.
//!
//! The registry lives in the server crate (not `core`) because metrics are
//! a server-side observability concern.

use axum::{
    extract::Request, http::StatusCode, middleware::Next, response::Response, routing::get, Json,
    Router,
};
use serde::Serialize;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Instant;

/// Simple atomic counter.
pub struct Counter {
    value: AtomicU64,
}

impl Counter {
    pub fn new() -> Self {
        Self {
            value: AtomicU64::new(0),
        }
    }
    pub fn inc(&self) {
        self.value.fetch_add(1, Ordering::Relaxed);
    }
    pub fn get(&self) -> u64 {
        self.value.load(Ordering::Relaxed)
    }
}

impl Default for Counter {
    fn default() -> Self {
        Self::new()
    }
}

/// Application-level metrics registry.
pub struct MetricsRegistry {
    http_requests: RwLock<HashMap<(String, String, u16), Counter>>,
    iceberg_commit_conflicts: Counter,
    iceberg_commit_successes: Counter,
}

impl MetricsRegistry {
    pub fn new() -> Self {
        Self {
            http_requests: RwLock::new(HashMap::new()),
            iceberg_commit_conflicts: Counter::new(),
            iceberg_commit_successes: Counter::new(),
        }
    }

    pub fn record_http_request(&self, method: &str, path: &str, status: u16) {
        let key = (method.to_string(), path.to_string(), status);
        // First try read lock to check if key exists
        {
            let map = self.http_requests.read().unwrap_or_else(|e| e.into_inner());
            if let Some(counter) = map.get(&key) {
                counter.inc();
                return;
            }
        }
        // Key doesn't exist, need write lock to insert
        let mut map = self
            .http_requests
            .write()
            .unwrap_or_else(|e| e.into_inner());
        // Double-check in case another thread inserted while we waited
        map.entry(key).or_default().inc();
    }

    pub fn record_iceberg_commit_conflict(&self) {
        self.iceberg_commit_conflicts.inc();
    }

    pub fn record_iceberg_commit_success(&self) {
        self.iceberg_commit_successes.inc();
    }

    pub fn render(&self) -> String {
        let mut output = String::new();

        output.push_str("# HELP http_requests_total Total HTTP requests\n");
        output.push_str("# TYPE http_requests_total counter\n");
        {
            let map = self.http_requests.read().unwrap_or_else(|e| e.into_inner());
            for ((method, path, status), counter) in map.iter() {
                output.push_str(&format!(
                    "http_requests_total{{method=\"{}\",path=\"{}\",status=\"{}\"}} {}\n",
                    method,
                    path,
                    status,
                    counter.get()
                ));
            }
        }

        output.push_str(
            "\n# HELP iceberg_commit_conflicts_total Total Iceberg commit CAS conflicts\n",
        );
        output.push_str("# TYPE iceberg_commit_conflicts_total counter\n");
        output.push_str(&format!(
            "iceberg_commit_conflicts_total {}\n",
            self.iceberg_commit_conflicts.get()
        ));

        output.push_str("\n# HELP iceberg_commit_successes_total Total Iceberg commit successes\n");
        output.push_str("# TYPE iceberg_commit_successes_total counter\n");
        output.push_str(&format!(
            "iceberg_commit_successes_total {}\n",
            self.iceberg_commit_successes.get()
        ));

        output
    }
}

impl Default for MetricsRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Shared metrics state (cloneable via Arc).
#[derive(Clone)]
pub struct MetricsState {
    pub registry: Arc<MetricsRegistry>,
}

impl Default for MetricsState {
    fn default() -> Self {
        Self {
            registry: Arc::new(MetricsRegistry::new()),
        }
    }
}

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
pub async fn request_id(mut req: Request, next: Next) -> Response {
    let request_id = req
        .headers()
        .get("x-request-id")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string())
        .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());

    req.extensions_mut().insert(request_id.clone());

    let mut response = next.run(req).await;
    if let Ok(val) = request_id.parse() {
        response.headers_mut().insert("x-request-id", val);
    }
    response
}

/// Normalize dynamic path segments for metrics labels to prevent cardinality
/// explosion. Every identifier segment (warehouse prefix, namespace path,
/// table name, asset id, ...) collapses to a fixed placeholder so the label
/// set stays bounded by the registered route surface.
fn normalize_path(path: &str) -> String {
    let segments: Vec<&str> = path.split('/').skip(1).collect();
    match segments.as_slice() {
        ["iceberg", "v1", "config"] => "/iceberg/v1/config".to_string(),
        ["iceberg", "v1", _, rest @ ..] if !rest.is_empty() => normalize_iceberg(rest),
        ["lance", "v1", kind @ ("namespace" | "table"), rest @ ..] if rest.len() >= 2 => {
            // The id may span multiple segments; the trailing segments form
            // the action (optionally nested as `table/list` or
            // `version/<verb>`).
            let n = rest.len();
            let action = if n >= 3 && matches!(rest[n - 2], "table" | "version") {
                format!("{}/{}", rest[n - 2], rest[n - 1])
            } else {
                rest[n - 1].to_string()
            };
            format!("/lance/v1/{}/{{id}}/{}", kind, action)
        }
        ["unified", "v1", rest @ ..] if !rest.is_empty() => normalize_unified(rest),
        _ => path.to_string(),
    }
}

/// Normalize the path tail after `/iceberg/v1/{prefix}`.
fn normalize_iceberg(rest: &[&str]) -> String {
    const PREFIX: &str = "/iceberg/v1/{prefix}";
    match rest {
        ["namespaces"] => format!("{PREFIX}/namespaces"),
        ["namespaces", tail @ ..] => {
            // The hierarchical namespace path precedes the first reserved
            // segment (`tables`/`views`/`properties`/`register`).
            let reserved = ["tables", "views", "properties", "register"];
            let mut out = format!("{PREFIX}/namespaces/{{ns}}");
            if let Some(i) = tail.iter().position(|s| reserved.contains(s)) {
                let kind = tail[i];
                out.push_str(&format!("/{}", kind));
                if matches!(kind, "tables" | "views") && tail.len() > i + 1 {
                    out.push_str("/{name}");
                    if tail.len() > i + 2 {
                        // Trailing action such as `metrics`.
                        out.push_str(&format!("/{}", tail[i + 2]));
                    }
                }
            }
            out
        }
        ["tables", "rename"] => format!("{PREFIX}/tables/rename"),
        ["transactions", "commit"] => format!("{PREFIX}/transactions/commit"),
        ["views", "rename"] => format!("{PREFIX}/views/rename"),
        _ => format!("{PREFIX}/{{path}}"),
    }
}

/// Normalize the path tail after `/unified/v1`.
fn normalize_unified(rest: &[&str]) -> String {
    const PREFIX: &str = "/unified/v1";
    match rest {
        ["domains"] | ["assets"] | ["asset-types"] | ["formats"] => {
            format!("{PREFIX}/{}", rest[0])
        }
        ["domains", _, tail @ ..] => {
            let mut out = format!("{PREFIX}/domains/{{domain}}");
            match tail {
                [] => {}
                ["namespaces"] => out.push_str("/namespaces"),
                ["namespaces", ..] => out.push_str("/namespaces/{ns}"),
                _ => out.push_str("/{path}"),
            }
            out
        }
        ["assets", _, tail @ ..] => {
            let mut out = format!("{PREFIX}/assets/{{asset_id}}");
            match tail {
                [] => {}
                ["restore"] => out.push_str("/restore"),
                ["tags"] => out.push_str("/tags"),
                ["tags", _] => out.push_str("/tags/{tag}"),
                ["versions"] => out.push_str("/versions"),
                ["versions", _] => out.push_str("/versions/{version_key}"),
                _ => out.push_str("/{path}"),
            }
            out
        }
        ["asset-types", _] => format!("{PREFIX}/asset-types/{{name}}"),
        ["formats", _] => format!("{PREFIX}/formats/{{name}}"),
        _ => format!("{PREFIX}/{{path}}"),
    }
}
