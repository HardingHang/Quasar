pub mod config;
pub mod health;

use axum::Router;
use deadpool_postgres::Pool;
use quasar_adapter::lance::LanceConfig;
use quasar_storage::PgCatalogStore;
use std::sync::Arc;
use tower_http::trace::TraceLayer;

pub fn create_app(pool: Pool) -> Router {
    create_app_with_config(pool, LanceConfig::default())
}

pub fn create_app_with_config(pool: Pool, lance_config: LanceConfig) -> Router {
    let store = Arc::new(PgCatalogStore::new(pool.clone()));
    Router::new()
        .merge(health::routes(pool))
        .merge(quasar_adapter::lance::routes(lance_config))
        .layer(TraceLayer::new_for_http())
        .with_state(store)
}
