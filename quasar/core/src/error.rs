use thiserror::Error;

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("not found: {0}")]
    NotFound(String),

    #[error("already exists: {0}")]
    AlreadyExists(String),

    #[error("conflict: {0}")]
    Conflict(String),

    #[error("invalid input: {0}")]
    InvalidInput(String),

    #[error("internal error: {0}")]
    Internal(String),
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let err = StoreError::Conflict("namespace not empty".to_string());
        assert_eq!(err.to_string(), "conflict: namespace not empty");
    }

    #[test]
    fn test_invalid_input_display() {
        let err = StoreError::InvalidInput("empty name".to_string());
        assert_eq!(err.to_string(), "invalid input: empty name");
    }

    #[test]
    fn test_internal_display() {
        let err = StoreError::Internal("db connection lost".to_string());
        assert_eq!(err.to_string(), "internal error: db connection lost");
    }
}
