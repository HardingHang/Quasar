use thiserror::Error;

/// Boxed source error used by structured `StoreError` variants.
///
/// Source errors must be `Send + Sync + 'static` so they can be moved across
/// async boundaries and reported through `tracing` without losing causality.
pub type BoxSource = Box<dyn std::error::Error + Send + Sync + 'static>;

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("not found: {0}")]
    NotFound(String),

    #[error("already exists: {0}")]
    AlreadyExists(String),

    #[error("conflict: {msg}")]
    Conflict { msg: String },

    #[error("invalid input: {0}")]
    InvalidInput(String),

    #[error("namespace not empty: {namespace}")]
    NamespaceNotEmpty { namespace: String },

    #[error("domain not empty: {domain}")]
    DomainNotEmpty { domain: String },

    #[error("database unavailable")]
    DatabaseUnavailable {
        #[source]
        source: Option<BoxSource>,
    },

    #[error("timeout: {operation}")]
    Timeout { operation: String },

    #[error("internal error: {msg}")]
    Internal {
        msg: String,
        #[source]
        source: Option<BoxSource>,
    },
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;
    use std::error::Error;

    #[test]
    fn test_not_found_display() {
        let err = StoreError::NotFound("namespace foo".to_string());
        assert_eq!(err.to_string(), "not found: namespace foo");
    }

    #[test]
    fn test_already_exists_display() {
        let err = StoreError::AlreadyExists("table bar".to_string());
        assert_eq!(err.to_string(), "already exists: table bar");
    }

    #[test]
    fn test_conflict_display() {
        let err = StoreError::Conflict {
            msg: "previous version 3 not found".to_string(),
        };
        assert_eq!(err.to_string(), "conflict: previous version 3 not found");
    }

    #[test]
    fn test_invalid_input_display() {
        let err = StoreError::InvalidInput("empty name".to_string());
        assert_eq!(err.to_string(), "invalid input: empty name");
    }

    #[test]
    fn test_namespace_not_empty_display() {
        let err = StoreError::NamespaceNotEmpty {
            namespace: "prod".to_string(),
        };
        assert_eq!(err.to_string(), "namespace not empty: prod");
    }

    #[test]
    fn test_domain_not_empty_display() {
        let err = StoreError::DomainNotEmpty {
            domain: "prod".to_string(),
        };
        assert_eq!(err.to_string(), "domain not empty: prod");
    }

    #[test]
    fn test_database_unavailable_display_without_source() {
        let err = StoreError::DatabaseUnavailable { source: None };
        assert_eq!(err.to_string(), "database unavailable");
        assert!(err.source().is_none());
    }

    #[test]
    fn test_database_unavailable_chains_source() {
        let cause = std::io::Error::new(std::io::ErrorKind::ConnectionRefused, "refused");
        let err = StoreError::DatabaseUnavailable {
            source: Some(Box::new(cause)),
        };
        let chained = err.source().unwrap();
        assert!(chained.to_string().contains("refused"));
    }

    #[test]
    fn test_timeout_display() {
        let err = StoreError::Timeout {
            operation: "list_namespaces".to_string(),
        };
        assert_eq!(err.to_string(), "timeout: list_namespaces");
    }

    #[test]
    fn test_internal_display_without_source() {
        let err = StoreError::Internal {
            msg: "db connection lost".to_string(),
            source: None,
        };
        assert_eq!(err.to_string(), "internal error: db connection lost");
        assert!(err.source().is_none());
    }

    #[test]
    fn test_internal_chains_source() {
        let cause = std::io::Error::other("inner");
        let err = StoreError::Internal {
            msg: "wrapper".to_string(),
            source: Some(Box::new(cause)),
        };
        let chained = err.source().unwrap();
        assert_eq!(chained.to_string(), "inner");
    }
}
