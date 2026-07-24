pub mod config;
pub mod health;
pub mod metrics;

use axum::{middleware, Extension, Router};
use deadpool_postgres::Pool;
#[cfg(any(feature = "lance", feature = "iceberg", feature = "unified"))]
use quasar_storage::PgCatalogStore;
#[cfg(any(feature = "lance", feature = "iceberg", feature = "unified"))]
use std::sync::Arc;
use tower_http::trace::TraceLayer;

#[cfg(feature = "iceberg")]
use quasar_adapter::iceberg::IcebergConfig;
#[cfg(feature = "lance")]
use quasar_adapter::lance::LanceConfig;
#[cfg(feature = "unified")]
use quasar_adapter::unified::UnifiedConfig;

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
    #[cfg(feature = "unified")]
    pub unified: UnifiedConfig,
}

pub fn create_app(pool: Pool) -> Router {
    create_app_with_config(pool, AppConfig::default())
}

pub fn create_app_with_config(pool: Pool, #[allow(unused_variables)] config: AppConfig) -> Router {
    #[cfg(any(feature = "lance", feature = "iceberg", feature = "unified"))]
    let store = Arc::new(PgCatalogStore::new(pool.clone()));
    let metrics_state = metrics::MetricsState::default();

    // Coerce the concrete store into the two trait-object shapes required by
    // the adapters: lance/unified operate on `CatalogStore`, iceberg on the
    // narrower `IcebergCatalogStore`.
    #[cfg(any(feature = "lance", feature = "unified"))]
    let catalog: Arc<dyn quasar_core::CatalogStore> = store.clone();
    #[cfg(feature = "iceberg")]
    let iceberg: Arc<dyn quasar_core::IcebergCatalogStore> = store.clone();

    #[allow(unused_mut)]
    let mut router = Router::new()
        .merge(health::routes(pool))
        .merge(metrics::routes());

    #[cfg(feature = "lance")]
    {
        router = router
            .merge(quasar_adapter::lance::routes().with_state(catalog.clone()))
            .layer(Extension(config.lance));
    }

    #[cfg(feature = "iceberg")]
    {
        // Adapt the server-side registry into the adapter-owned sink so the
        // adapter never depends on the server crate (layering rule).
        let commit_metrics = {
            let on_success = metrics_state.registry.clone();
            let on_conflict = metrics_state.registry.clone();
            quasar_adapter::iceberg::CommitMetrics::new(
                move || on_success.record_iceberg_commit_success(),
                move || on_conflict.record_iceberg_commit_conflict(),
            )
        };
        router = router
            .merge(quasar_adapter::iceberg::routes().with_state(iceberg))
            .layer(Extension(commit_metrics))
            .layer(Extension(config.iceberg));
    }

    #[cfg(feature = "unified")]
    {
        router = router
            .merge(quasar_adapter::unified::routes().with_state(catalog))
            .layer(Extension(config.unified));
    }

    router
        .layer(middleware::from_fn(metrics::request_id))
        .layer(middleware::from_fn(metrics::track_requests))
        .layer(TraceLayer::new_for_http())
        .layer(Extension(metrics_state))
}
