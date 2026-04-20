pub mod config;
pub mod health;

use axum::Router;
use deadpool_postgres::Pool;
use quasar_storage::PgCatalogStore;
use std::sync::Arc;
use tower_http::trace::TraceLayer;

pub fn create_app(pool: Pool) -> Router {
    let store = Arc::new(PgCatalogStore::new(pool.clone()));
    Router::new()
        .merge(health::routes(pool))
        .merge(quasar_adapter::lance::routes())
        .layer(TraceLayer::new_for_http())
        .with_state(store)
}
