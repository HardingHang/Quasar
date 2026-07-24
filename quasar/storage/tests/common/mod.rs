//! Shared integration-test bootstrap.
//!
//! Each test binary starts one embedded PostgreSQL instance (zonky
//! binaries, cached under `~/.postgresql_embedded`) and reuses it across
//! the binary's tests. Every test resets the public schema and re-runs the
//! migration runner via `PgCatalogStore::initialize`, so tests are
//! independent as long as they run serially (`#[serial]`).

#![allow(dead_code, clippy::unwrap_used, clippy::expect_used)]

use deadpool_postgres::{Pool, Runtime};
use postgresql_embedded::{PostgreSQL, Settings};
use quasar_storage::{
    AssetWithTabular, CreateAsset, CreateNamespace, NamespaceStore, PgCatalogStore, TabularStore,
};
use tokio::sync::OnceCell;

static PG_INSTANCE: OnceCell<PgInstance> = OnceCell::const_new();

/// zonky embedded-postgres-binaries repository (postgresql_archive
/// `configuration::zonky::URL`).
const ZONKY_RELEASES_URL: &str = "https://github.com/zonkyio/embedded-postgres-binaries";

struct PgInstance {
    // Kept to extend the embedded Postgres lifetime across the test run.
    #[allow(dead_code)]
    postgresql: PostgreSQL,
    url: String,
}

impl PgInstance {
    async fn get() -> &'static Self {
        PG_INSTANCE
            .get_or_init(|| async {
                // The dev-dependency disables postgresql_embedded's default
                // features (theseus) and keeps only `zonky`, which leaves
                // `Settings::default().releases_url` empty; point it at the
                // zonky binaries repository explicitly.
                let settings = Settings {
                    releases_url: ZONKY_RELEASES_URL.to_string(),
                    ..Default::default()
                };
                let mut postgresql = PostgreSQL::new(settings);
                postgresql.setup().await.expect("PostgreSQL setup failed");
                postgresql.start().await.expect("PostgreSQL start failed");
                postgresql
                    .create_database("quasar_test")
                    .await
                    .expect("create database failed");
                let url = postgresql.settings().url("quasar_test");
                PgInstance { postgresql, url }
            })
            .await
    }
}

fn test_pool(url: &str) -> Pool {
    let config = url
        .parse::<tokio_postgres::Config>()
        .expect("invalid database URL");
    let mgr = deadpool_postgres::Manager::new(config, tokio_postgres::NoTls);
    Pool::builder(mgr)
        .runtime(Runtime::Tokio1)
        .build()
        .expect("failed to create pool")
}

/// Reset the database to a clean, fully-migrated state.
///
/// Returns the store plus the pool so tests can run raw SQL assertions
/// against objects that have no trait-level accessor (staged table expiry,
/// metrics reports, purge operations).
pub async fn setup() -> (PgCatalogStore, Pool) {
    let instance = PgInstance::get().await;
    let pool = test_pool(&instance.url);
    {
        let client = pool.get().await.expect("pool checkout failed");
        client
            .batch_execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
            .await
            .expect("schema reset failed");
    }
    let store = PgCatalogStore::new(pool.clone());
    store.initialize().await.expect("initialize failed");
    (store, pool)
}

/// Clean store without pool access.
pub async fn store() -> PgCatalogStore {
    setup().await.0
}

/// Build a `CreateAsset` with only the required identity fields set.
pub fn asset_input(
    domain: &str,
    namespace: &str,
    name: &str,
    asset_type: &str,
    format: Option<&str>,
) -> CreateAsset {
    CreateAsset {
        domain: domain.to_string(),
        namespace: namespace.to_string(),
        name: name.to_string(),
        asset_type: asset_type.to_string(),
        format: format.map(str::to_string),
        ..Default::default()
    }
}

/// Create a namespace (including implicit intermediates) with defaults.
pub async fn make_namespace(store: &PgCatalogStore, domain: &str, path: &str) {
    store
        .create_namespace(domain, path, CreateNamespace::default())
        .await
        .expect("create namespace failed");
}

/// Create a `table` asset with its tabular extension row.
pub async fn make_tabular_asset(
    store: &PgCatalogStore,
    domain: &str,
    namespace: &str,
    name: &str,
    format: Option<&str>,
    location: &str,
    metadata_location: Option<&str>,
) -> AssetWithTabular {
    store
        .create_tabular_asset(
            asset_input(domain, namespace, name, "table", format),
            location,
            metadata_location,
        )
        .await
        .expect("create tabular asset failed")
}
