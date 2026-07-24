//! Input validation utilities for names and namespace paths.
//!
//! Naming rule (DESIGN §3.2, REQUIREMENTS §5.3/§5.4): URL-safe slug
//! `^[a-z0-9][a-z0-9_-]{0,62}$` — lowercase letters, digits, hyphens and
//! underscores, starting with a letter or digit, 1 to 63 characters.

use crate::error::CatalogError;

const MAX_SLUG_LENGTH: usize = 63;

fn is_slug_first_char(c: char) -> bool {
    c.is_ascii_lowercase() || c.is_ascii_digit()
}

fn is_slug_char(c: char) -> bool {
    is_slug_first_char(c) || c == '_' || c == '-'
}

/// Validate a URL-safe slug (Domain name, Namespace path segment, Asset name).
///
/// Rule: `^[a-z0-9][a-z0-9_-]{0,62}$`.
pub fn validate_name(name: &str) -> Result<(), CatalogError> {
    if name.is_empty() {
        return Err(CatalogError::Validation(
            "name must not be empty".to_string(),
        ));
    }
    if name.len() > MAX_SLUG_LENGTH {
        return Err(CatalogError::Validation(format!(
            "name exceeds maximum length of {} characters",
            MAX_SLUG_LENGTH
        )));
    }
    let mut chars = name.chars();
    // Non-empty is checked above, so `next` always yields the first char.
    if let Some(first) = chars.next() {
        if !is_slug_first_char(first) {
            return Err(CatalogError::Validation(format!(
                "name must start with a lowercase letter or digit, got '{}'",
                first
            )));
        }
    }
    if let Some(ch) = chars.find(|c| !is_slug_char(*c)) {
        return Err(CatalogError::Validation(format!(
            "name contains invalid character '{}'",
            ch
        )));
    }
    Ok(())
}

/// Validate a hierarchical namespace path (e.g. `analytics/teams/finance`).
///
/// The path is split on `/` and every segment must be a valid slug.
pub fn validate_namespace_path(path: &str) -> Result<(), CatalogError> {
    if path.is_empty() {
        return Err(CatalogError::Validation(
            "namespace path must not be empty".to_string(),
        ));
    }
    for segment in path.split('/') {
        validate_name(segment).map_err(|e| {
            CatalogError::Validation(format!("invalid namespace path '{}': {}", path, e))
        })?;
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
        assert!(validate_name("a").is_ok());
        assert!(validate_name("0").is_ok());
        assert!(validate_name("a1_-2").is_ok());
    }

    #[test]
    fn test_empty_name_fails() {
        let err = validate_name("").unwrap_err();
        assert!(matches!(err, CatalogError::Validation(ref msg) if msg.contains("empty")));
    }

    #[test]
    fn test_too_long_name_fails() {
        let long_name = "a".repeat(64);
        let err = validate_name(&long_name).unwrap_err();
        assert!(matches!(err, CatalogError::Validation(ref msg) if msg.contains("exceeds")));
    }

    #[test]
    fn test_max_length_ok() {
        let max_name = "a".repeat(63);
        assert!(validate_name(&max_name).is_ok());
    }

    #[test]
    fn test_invalid_first_char_fails() {
        for name in ["-abc", "_abc", ".hidden", "Abc"] {
            assert!(
                matches!(validate_name(name), Err(CatalogError::Validation(_))),
                "expected '{}' to fail",
                name
            );
        }
    }

    #[test]
    fn test_invalid_characters_fail() {
        for name in ["hello world", "hello\nworld", "hello$world", "UPPER", "a.b"] {
            assert!(
                matches!(validate_name(name), Err(CatalogError::Validation(_))),
                "expected '{}' to fail",
                name
            );
        }
    }

    #[test]
    fn test_valid_namespace_paths() {
        assert!(validate_namespace_path("analytics").is_ok());
        assert!(validate_namespace_path("analytics/teams/finance").is_ok());
        assert!(validate_namespace_path("a/b-c/d_e/0").is_ok());
    }

    #[test]
    fn test_invalid_namespace_paths() {
        for path in [
            "",
            "/analytics",
            "analytics/",
            "analytics//teams",
            "Analytics/teams",
            "analytics/te ams",
        ] {
            assert!(
                matches!(
                    validate_namespace_path(path),
                    Err(CatalogError::Validation(_))
                ),
                "expected '{}' to fail",
                path
            );
        }
    }

    #[test]
    fn test_namespace_path_error_mentions_path() {
        let err = validate_namespace_path("good/Bad").unwrap_err();
        assert!(matches!(err, CatalogError::Validation(ref msg) if msg.contains("good/Bad")));
    }
}
