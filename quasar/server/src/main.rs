#[cfg(feature = "lance")]
use std::collections::HashMap;
use std::net::SocketAddr;
use tracing::info;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cfg = quasar_server::config::Config::from_env();

    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(&cfg.log_level));
    tracing_subscriber::fmt()
        .with_env_filter(env_filter)
        .init();

    let pg_config: tokio_postgres::Config = cfg.database_url.parse()?;
    let mgr = deadpool_postgres::Manager::new(pg_config, tokio_postgres::NoTls);
    let pool = deadpool_postgres::Pool::builder(mgr).build()?;

    let store = std::sync::Arc::new(quasar_storage::PgCatalogStore::new(pool.clone()));
    store.migrate().await?;

    #[allow(unused_mut)]
    let mut app_config = quasar_server::AppConfig::default();

    #[cfg(feature = "lance")]
    {
        let mut storage_options = HashMap::new();
        if let Some(ref endpoint) = cfg.s3_endpoint {
            storage_options.insert("endpoint".to_string(), endpoint.clone());
        }
        if let Some(ref access_key) = cfg.s3_access_key {
            storage_options.insert("access_key_id".to_string(), access_key.clone());
        }
        if let Some(ref secret_key) = cfg.s3_secret_key {
            storage_options.insert("secret_access_key".to_string(), secret_key.clone());
        }
        if !cfg.s3_region.is_empty() {
            storage_options.insert("region".to_string(), cfg.s3_region.clone());
        }
        if cfg.s3_allow_http {
            storage_options.insert("allow_http".to_string(), "true".to_string());
        }

        app_config.lance = quasar_adapter::lance::LanceConfig {
            warehouse_path: cfg.warehouse_path.clone(),
            storage_options,
        };
    }

    #[cfg(feature = "iceberg")]
    {
        // Build S3 object store client for Iceberg
        let (object_store, s3_bucket) =
            if let (Some(endpoint), Some(access_key), Some(secret_key)) =
                (&cfg.s3_endpoint,
                 &cfg.s3_access_key,
                 &cfg.s3_secret_key)
            {
                let bucket = cfg
                    .warehouse_path
                    .as_ref()
                    .map(|wp| parse_s3_bucket(wp))
                    .unwrap_or_else(|| "warehouse".to_string());
                let store = object_store::aws::AmazonS3Builder::new()
                    .with_endpoint(endpoint)
                    .with_access_key_id(access_key)
                    .with_secret_access_key(secret_key)
                    .with_region(&cfg.s3_region)
                    .with_bucket_name(&bucket)
                    .with_virtual_hosted_style_request(false)
                    .with_allow_http(cfg.s3_allow_http)
                    .build()?;
                (
                    Some(std::sync::Arc::new(store) as std::sync::Arc<dyn object_store::ObjectStore>),
                    Some(bucket),
                )
            } else {
                (None, None)
            };

        app_config.iceberg = quasar_adapter::iceberg::IcebergConfig {
            warehouse_path: cfg.warehouse_path,
            object_store,
            s3_bucket,
        };
    }

    let app = quasar_server::create_app_with_config(pool, app_config);

    let addr = SocketAddr::from((cfg.host.parse::<std::net::IpAddr>()?, cfg.port));
    info!("Quasar server listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}

#[cfg(feature = "iceberg")]
fn parse_s3_bucket(warehouse_path: &str) -> String {
    if let Some(rest) = warehouse_path.strip_prefix("s3://") {
        let parts: Vec<&str> = rest.splitn(2, '/').collect();
        if !parts[0].is_empty() {
            return parts[0].to_string();
        }
    }
    "warehouse".to_string()
}

async fn shutdown_signal() {
    let ctrl_c = async {
        if let Err(e) = tokio::signal::ctrl_c().await {
            tracing::error!("failed to install Ctrl+C handler: {}", e);
        }
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut stream) => {
                stream.recv().await;
            }
            Err(e) => {
                tracing::error!("failed to install signal handler: {}", e);
            }
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    info!("shutdown signal received, shutting down gracefully");
}
