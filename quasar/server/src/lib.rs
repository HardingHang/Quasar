pub mod config;
pub mod health;

use axum::Router;
use deadpool_postgres::Pool;
use quasar_storage::PgCatalogStore;
use std::sync::Arc;
use tower_http::trace::TraceLayer;

#[cfg(feature = "iceberg")]
use quasar_adapter::iceberg::IcebergConfig;
#[cfg(feature = "lance")]
use quasar_adapter::lance::LanceConfig;

/// Application configuration that controls which catalog protocols are enabled.
///
/// Each field is conditionally compiled based on the corresponding Cargo feature.
/// Use `AppConfig::default()` to get default (empty) configs for all enabled features.
#[derive(Clone, Default)]
pub struct AppConfig {
    #[cfg(feature = "lance")]
    pub lance: LanceConfig,
    #[cfg(feature = "iceberg")]
    pub iceberg: IcebergConfig,
}

pub fn create_app(pool: Pool) -> Router {
    create_app_with_config(pool, AppConfig::default())
}

pub fn create_app_with_config(pool: Pool, #[allow(unused_variables)] config: AppConfig) -> Router {
    let store = Arc::new(PgCatalogStore::new(pool.clone()));
    #[allow(unused_mut)]
    let mut router = Router::new().merge(health::routes(pool));

    #[cfg(feature = "lance")]
    {
        router = router.merge(quasar_adapter::lance::routes(config.lance));
    }

    #[cfg(feature = "iceberg")]
    {
        router = router.merge(quasar_adapter::iceberg::routes(config.iceberg));
    }

    router.layer(TraceLayer::new_for_http()).with_state(store)
}
