use axum::{routing::get, Router};
use quasar_storage::PgCatalogStore;
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::info;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let database_url = std::env::var("QUASAR_DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/quasar".to_string());

    let config: tokio_postgres::Config = database_url.parse()?;
    let mgr = deadpool_postgres::Manager::new(config, tokio_postgres::NoTls);
    let pool = deadpool_postgres::Pool::builder(mgr).build()?;

    let store = Arc::new(PgCatalogStore::new(pool));
    store.migrate().await?;

    let app = Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .merge(quasar_adapter::lance::routes())
        .with_state(store);

    let addr = SocketAddr::from(([0, 0, 0, 0], 8080));
    info!("Quasar server listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}
