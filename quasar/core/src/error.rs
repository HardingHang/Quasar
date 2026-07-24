//! Unified error type for all store traits (DESIGN §7.4).

use thiserror::Error;

/// Unified error returned by every store trait.
///
/// Adapters map variants to protocol-specific status codes and error
/// bodies (DESIGN §7.5). Carried context is for logs only; responses
/// must be sanitized (DESIGN §7.3).
#[derive(Debug, Error)]
pub enum CatalogError {
    /// Target resource does not exist (Domain, Namespace, Asset, Version, ...).
    #[error("not found: {0}")]
    NotFound(String),

    /// Creation violated a uniqueness constraint (duplicate asset name, tag, ...).
    #[error("already exists: {0}")]
    AlreadyExists(String),

    /// Concurrency or state conflict (CAS failure, restore name clash, ...).
    #[error("conflict: {0}")]
    Conflict(String),

    /// Invalid request parameters (naming rules, required fields, format constraints).
    #[error("validation failed: {0}")]
    Validation(String),

    /// Retryable transient error (connection timeout, pool exhaustion, serialization conflict).
    #[error("transient error: {0}")]
    Transient(String),

    /// Unexpected internal error; context is for logs only and must be sanitized in responses.
    #[error("internal error: {0}")]
    Internal(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_not_found_display() {
        let err = CatalogError::NotFound("namespace foo".to_string());
        assert_eq!(err.to_string(), "not found: namespace foo");
    }

    #[test]
    fn test_already_exists_display() {
        let err = CatalogError::AlreadyExists("table bar".to_string());
        assert_eq!(err.to_string(), "already exists: table bar");
    }

    #[test]
    fn test_conflict_display() {
        let err = CatalogError::Conflict("CAS pointer mismatch".to_string());
        assert_eq!(err.to_string(), "conflict: CAS pointer mismatch");
    }

    #[test]
    fn test_validation_display() {
        let err = CatalogError::Validation("empty name".to_string());
        assert_eq!(err.to_string(), "validation failed: empty name");
    }

    #[test]
    fn test_transient_display() {
        let err = CatalogError::Transient("connection timeout".to_string());
        assert_eq!(err.to_string(), "transient error: connection timeout");
    }

    #[test]
    fn test_internal_display() {
        let err = CatalogError::Internal("db connection lost".to_string());
        assert_eq!(err.to_string(), "internal error: db connection lost");
    }
}
