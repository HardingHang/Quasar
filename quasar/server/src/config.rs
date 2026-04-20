pub struct Config {
    pub host: String,
    pub port: u16,
    pub database_url: String,
    pub log_level: String,
    pub warehouse_path: Option<String>,
    pub s3_endpoint: Option<String>,
    pub s3_access_key: Option<String>,
    pub s3_secret_key: Option<String>,
    pub s3_region: String,
    pub s3_allow_http: bool,
}

impl Config {
    pub fn from_env() -> Self {
        Self {
            host: std::env::var("QUASAR_HOST").unwrap_or_else(|_| "0.0.0.0".to_string()),
            port: std::env::var("QUASAR_PORT")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(8080),
            database_url: std::env::var("QUASAR_DATABASE_URL").unwrap_or_else(|_| {
                "postgres://postgres:postgres@localhost:5432/quasar".to_string()
            }),
            log_level: std::env::var("QUASAR_LOG_LEVEL")
                .or_else(|_| std::env::var("RUST_LOG"))
                .unwrap_or_else(|_| "info".to_string()),
            warehouse_path: std::env::var("QUASAR_WAREHOUSE_PATH").ok(),
            s3_endpoint: std::env::var("QUASAR_S3_ENDPOINT").ok(),
            s3_access_key: std::env::var("QUASAR_S3_ACCESS_KEY").ok(),
            s3_secret_key: std::env::var("QUASAR_S3_SECRET_KEY").ok(),
            s3_region: std::env::var("QUASAR_S3_REGION").unwrap_or_else(|_| "us-east-1".to_string()),
            s3_allow_http: std::env::var("QUASAR_S3_ALLOW_HTTP")
                .map(|v| v == "true" || v == "1")
                .unwrap_or(false),
        }
    }
}
