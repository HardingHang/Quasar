use axum::{
    extract::Extension,
    http::StatusCode,
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use deadpool_postgres::Pool;
use serde::Serialize;
use tracing::error;

#[derive(Serialize)]
pub struct HealthResponse {
    pub status: String,
}

#[derive(Serialize)]
pub struct ReadyResponse {
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub checks: Option<Checks>,
}

#[derive(Serialize)]
pub struct Checks {
    pub database: String,
}

pub fn routes<S>(pool: Pool) -> Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .layer(Extension(pool))
}

async fn healthz() -> impl IntoResponse {
    Json(HealthResponse {
        status: "ok".to_string(),
    })
}

async fn readyz(Extension(pool): Extension<Pool>) -> impl IntoResponse {
    match pool.get().await {
        Ok(client) => match client.query_one("SELECT 1", &[]).await {
            Ok(_) => (
                StatusCode::OK,
                Json(ReadyResponse {
                    status: "ready".to_string(),
                    checks: Some(Checks {
                        database: "ok".to_string(),
                    }),
                }),
            ),
            Err(e) => {
                error!("readiness check failed: database query error: {}", e);
                (
                    StatusCode::SERVICE_UNAVAILABLE,
                    Json(ReadyResponse {
                        status: "not_ready".to_string(),
                        checks: Some(Checks {
                            database: "unreachable".to_string(),
                        }),
                    }),
                )
            }
        },
        Err(e) => {
            error!("readiness check failed: cannot get db connection: {}", e);
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ReadyResponse {
                    status: "not_ready".to_string(),
                    checks: Some(Checks {
                        database: "unreachable".to_string(),
                    }),
                }),
            )
        }
    }
}
