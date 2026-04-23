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
    pub db_max_connections: usize,
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
            db_max_connections: std::env::var("QUASAR_DB_MAX_CONNECTIONS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(10),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    fn clear_quasar_env() {
        for key in [
            "QUASAR_HOST",
            "QUASAR_PORT",
            "QUASAR_DATABASE_URL",
            "QUASAR_LOG_LEVEL",
            "RUST_LOG",
            "QUASAR_WAREHOUSE_PATH",
            "QUASAR_S3_ENDPOINT",
            "QUASAR_S3_ACCESS_KEY",
            "QUASAR_S3_SECRET_KEY",
            "QUASAR_S3_REGION",
            "QUASAR_S3_ALLOW_HTTP",
            "QUASAR_DB_MAX_CONNECTIONS",
        ] {
            std::env::remove_var(key);
        }
    }

    #[test]
    #[serial]
    fn test_config_defaults() {
        clear_quasar_env();
        let cfg = Config::from_env();

        assert_eq!(cfg.host, "0.0.0.0");
        assert_eq!(cfg.port, 8080);
        assert_eq!(cfg.database_url, "postgres://postgres:postgres@localhost:5432/quasar");
        assert_eq!(cfg.log_level, "info");
        assert_eq!(cfg.s3_region, "us-east-1");
        assert!(!cfg.s3_allow_http);
        assert_eq!(cfg.db_max_connections, 10);
        assert!(cfg.warehouse_path.is_none());
        assert!(cfg.s3_endpoint.is_none());
        assert!(cfg.s3_access_key.is_none());
        assert!(cfg.s3_secret_key.is_none());
    }

    #[test]
    #[serial]
    fn test_config_custom_values() {
        clear_quasar_env();
        std::env::set_var("QUASAR_HOST", "127.0.0.1");
        std::env::set_var("QUASAR_PORT", "9090");
        std::env::set_var("QUASAR_DATABASE_URL", "postgres://user:pass@db:5432/test");
        std::env::set_var("QUASAR_LOG_LEVEL", "debug");
        std::env::set_var("QUASAR_WAREHOUSE_PATH", "s3://bucket/warehouse");
        std::env::set_var("QUASAR_S3_ENDPOINT", "http://localhost:9000");
        std::env::set_var("QUASAR_S3_ACCESS_KEY", "minioadmin");
        std::env::set_var("QUASAR_S3_SECRET_KEY", "minioadmin");
        std::env::set_var("QUASAR_S3_REGION", "us-west-2");
        std::env::set_var("QUASAR_S3_ALLOW_HTTP", "true");

        let cfg = Config::from_env();

        assert_eq!(cfg.host, "127.0.0.1");
        assert_eq!(cfg.port, 9090);
        assert_eq!(cfg.database_url, "postgres://user:pass@db:5432/test");
        assert_eq!(cfg.log_level, "debug");
        assert_eq!(cfg.warehouse_path, Some("s3://bucket/warehouse".to_string()));
        assert_eq!(cfg.s3_endpoint, Some("http://localhost:9000".to_string()));
        assert_eq!(cfg.s3_access_key, Some("minioadmin".to_string()));
        assert_eq!(cfg.s3_secret_key, Some("minioadmin".to_string()));
        assert_eq!(cfg.s3_region, "us-west-2");
        assert!(cfg.s3_allow_http);

        clear_quasar_env();
    }

    #[test]
    #[serial]
    fn test_config_log_level_falls_back_to_rust_log() {
        clear_quasar_env();
        std::env::set_var("RUST_LOG", "warn");

        let cfg = Config::from_env();
        assert_eq!(cfg.log_level, "warn");

        clear_quasar_env();
    }

    #[test]
    #[serial]
    fn test_config_quasar_log_level_takes_precedence_over_rust_log() {
        clear_quasar_env();
        std::env::set_var("QUASAR_LOG_LEVEL", "trace");
        std::env::set_var("RUST_LOG", "error");

        let cfg = Config::from_env();
        assert_eq!(cfg.log_level, "trace");

        clear_quasar_env();
    }

    #[test]
    #[serial]
    fn test_config_invalid_port_uses_default() {
        clear_quasar_env();
        std::env::set_var("QUASAR_PORT", "not_a_number");

        let cfg = Config::from_env();
        assert_eq!(cfg.port, 8080);

        clear_quasar_env();
    }

    #[test]
    #[serial]
    fn test_config_s3_allow_http_numeric_true() {
        clear_quasar_env();
        std::env::set_var("QUASAR_S3_ALLOW_HTTP", "1");

        let cfg = Config::from_env();
        assert!(cfg.s3_allow_http);

        clear_quasar_env();
    }

    #[test]
    #[serial]
    fn test_config_s3_allow_http_false() {
        clear_quasar_env();
        std::env::set_var("QUASAR_S3_ALLOW_HTTP", "false");

        let cfg = Config::from_env();
        assert!(!cfg.s3_allow_http);

        clear_quasar_env();
    }

    #[test]
    #[serial]
    fn test_config_db_max_connections_default() {
        clear_quasar_env();
        let cfg = Config::from_env();
        assert_eq!(cfg.db_max_connections, 10);
    }

    #[test]
    #[serial]
    fn test_config_db_max_connections_custom() {
        clear_quasar_env();
        std::env::set_var("QUASAR_DB_MAX_CONNECTIONS", "50");

        let cfg = Config::from_env();
        assert_eq!(cfg.db_max_connections, 50);

        clear_quasar_env();
    }
}
