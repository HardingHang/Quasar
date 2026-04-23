//! Input validation utilities for namespace and table names.

use crate::error::StoreError;

const MAX_NAME_LENGTH: usize = 256;
const VALID_NAME_CHARS: &str = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-./";

/// Validate a namespace or table name.
///
/// Rules:
/// - Length: 1 to 256 characters
/// - Allowed characters: alphanumeric, underscore, hyphen, dot, forward slash
/// - Must not start with a dot (reserved for hidden names)
/// - Must not be empty
pub fn validate_name(name: &str) -> Result<(), StoreError> {
    if name.is_empty() {
        return Err(StoreError::InvalidInput(
            "name must not be empty".to_string(),
        ));
    }
    if name.len() > MAX_NAME_LENGTH {
        return Err(StoreError::InvalidInput(format!(
            "name exceeds maximum length of {} characters",
            MAX_NAME_LENGTH
        )));
    }
    if name.starts_with('.') {
        return Err(StoreError::InvalidInput(
            "name must not start with a dot".to_string(),
        ));
    }
    if let Some(ch) = name.chars().find(|c| !VALID_NAME_CHARS.contains(*c)) {
        return Err(StoreError::InvalidInput(format!(
            "name contains invalid character '{}'",
            ch
        )));
    }
    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn test_valid_names() {
        assert!(validate_name("prod").is_ok());
        assert!(validate_name("users_table").is_ok());
        assert!(validate_name("users-table").is_ok());
        assert!(validate_name("users.table").is_ok());
        assert!(validate_name("prod/users").is_ok());
        assert!(validate_name("a").is_ok());
    }

    #[test]
    fn test_empty_name_fails() {
        let err = validate_name("").unwrap_err();
        assert!(matches!(err, StoreError::InvalidInput(ref msg) if msg.contains("empty")));
    }

    #[test]
    fn test_too_long_name_fails() {
        let long_name = "a".repeat(257);
        let err = validate_name(&long_name).unwrap_err();
        assert!(matches!(err, StoreError::InvalidInput(ref msg) if msg.contains("exceeds")));
    }

    #[test]
    fn test_max_length_ok() {
        let max_name = "a".repeat(256);
        assert!(validate_name(&max_name).is_ok());
    }

    #[test]
    fn test_dot_prefix_fails() {
        let err = validate_name(".hidden").unwrap_err();
        assert!(matches!(err, StoreError::InvalidInput(ref msg) if msg.contains("dot")));
    }

    #[test]
    fn test_invalid_characters_fails() {
        let err = validate_name("hello world").unwrap_err();
        assert!(
            matches!(err, StoreError::InvalidInput(ref msg) if msg.contains("invalid character"))
        );

        let err = validate_name("hello\nworld").unwrap_err();
        assert!(
            matches!(err, StoreError::InvalidInput(ref msg) if msg.contains("invalid character"))
        );

        let err = validate_name("hello$world").unwrap_err();
        assert!(
            matches!(err, StoreError::InvalidInput(ref msg) if msg.contains("invalid character"))
        );
    }
}
