pub struct Config {
    pub host: String,
    pub port: u16,
    pub database_url: String,
    pub log_level: String,
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
        }
    }
}
