//! Lance namespace and table identifier parsing.
//!
//! V3 encodes the Quasar Domain as the first segment of the Lance `{id}`
//! path parameter using `'$'` as delimiter. See `docs/v3/V3_DESIGN.md`
//! §4.1.2 for the mapping:
//!
//! | Quasar object                                  | Lance `{id}`                |
//! | ---------------------------------------------- | --------------------------- |
//! | Root (service discovery)                       | `"$"`                       |
//! | Domain `prod`                                  | `"prod"`                    |
//! | Domain `prod` + Namespace `analytics`          | `"prod$analytics"`          |
//! | Domain + Namespace + Table `embeddings`        | `"prod$analytics$embeddings"` |
//!
//! Phase 3 C2 lands the parser; the call sites in `namespace.rs` /
//! `table.rs` / `version.rs` are wired in C3 / C4.
#![allow(dead_code)]

use super::error::LanceError;

const DELIMITER: char = '$';

/// Parsed Lance namespace identifier.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum LanceNamespaceId {
    /// `"$"` – service-discovery root. Only `list` returns the Domain list;
    /// every other handler must reject Root.
    Root,
    /// One non-empty segment – addresses a Quasar Domain. V3 forbids Lance
    /// clients from managing Domains; the handler must surface `InvalidInput`.
    Domain(String),
    /// Two non-empty segments – addresses a Quasar Namespace within a Domain.
    Namespace { domain: String, namespace: String },
}

/// Parsed Lance table identifier. Always three non-empty segments.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LanceTableId {
    pub domain: String,
    pub namespace: String,
    pub table: String,
}

/// Parse a Lance namespace `{id}` parameter.
///
/// `instance` is the request path for the `ProblemDetails` response in the
/// case of an error.
pub(crate) fn parse_namespace_id(id: &str, instance: &str) -> Result<LanceNamespaceId, LanceError> {
    if id == "$" {
        return Ok(LanceNamespaceId::Root);
    }

    let segments: Vec<&str> = id.split(DELIMITER).collect();

    if segments.iter().any(|s| s.is_empty()) {
        return Err(invalid_namespace_id(id, instance));
    }

    match segments.as_slice() {
        [domain] => Ok(LanceNamespaceId::Domain((*domain).to_string())),
        [domain, namespace] => Ok(LanceNamespaceId::Namespace {
            domain: (*domain).to_string(),
            namespace: (*namespace).to_string(),
        }),
        _ => Err(invalid_namespace_id(id, instance)),
    }
}

/// Parse a Lance table `{id}` parameter. Must be exactly three non-empty
/// segments: `domain$namespace$table`.
pub(crate) fn parse_table_id(id: &str, instance: &str) -> Result<LanceTableId, LanceError> {
    let segments: Vec<&str> = id.split(DELIMITER).collect();
    if segments.len() != 3 || segments.iter().any(|s| s.is_empty()) {
        return Err(LanceError::InvalidInput {
            detail: format!(
                "invalid table id '{}': expected '{{domain}}${{namespace}}${{table}}'",
                id
            ),
            instance: instance.to_string(),
        });
    }
    Ok(LanceTableId {
        domain: segments[0].to_string(),
        namespace: segments[1].to_string(),
        table: segments[2].to_string(),
    })
}

fn invalid_namespace_id(id: &str, instance: &str) -> LanceError {
    LanceError::InvalidInput {
        detail: format!(
            "invalid namespace id '{}': expected '$' (root), \
             '{{domain}}', or '{{domain}}${{namespace}}'",
            id
        ),
        instance: instance.to_string(),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    const INSTANCE: &str = "/lance/v1/namespace/<id>/test";

    #[test]
    fn parse_namespace_id_root() {
        let parsed = parse_namespace_id("$", INSTANCE).ok();
        assert_eq!(parsed, Some(LanceNamespaceId::Root));
    }

    #[test]
    fn parse_namespace_id_single_segment_is_domain() {
        let parsed = parse_namespace_id("prod", INSTANCE).ok();
        assert_eq!(parsed, Some(LanceNamespaceId::Domain("prod".to_string())));
    }

    #[test]
    fn parse_namespace_id_two_segments_is_namespace() {
        let parsed = parse_namespace_id("prod$analytics", INSTANCE).ok();
        assert_eq!(
            parsed,
            Some(LanceNamespaceId::Namespace {
                domain: "prod".to_string(),
                namespace: "analytics".to_string(),
            })
        );
    }

    #[test]
    fn parse_namespace_id_three_segments_rejected() {
        assert!(parse_namespace_id("a$b$c", INSTANCE).is_err());
    }

    #[test]
    fn parse_namespace_id_four_segments_rejected() {
        assert!(parse_namespace_id("a$b$c$d", INSTANCE).is_err());
    }

    #[test]
    fn parse_namespace_id_empty_inner_segment_rejected() {
        assert!(parse_namespace_id("a$$b", INSTANCE).is_err());
    }

    #[test]
    fn parse_namespace_id_trailing_empty_segment_rejected() {
        assert!(parse_namespace_id("a$", INSTANCE).is_err());
    }

    #[test]
    fn parse_namespace_id_leading_empty_segment_rejected() {
        // "$$" splits to ["", "", ""] which has empty segments and is not the Root sentinel.
        assert!(parse_namespace_id("$$", INSTANCE).is_err());
    }

    #[test]
    fn parse_table_id_three_segments_accepted() {
        let parsed = parse_table_id("prod$analytics$events", INSTANCE).ok();
        assert_eq!(
            parsed,
            Some(LanceTableId {
                domain: "prod".to_string(),
                namespace: "analytics".to_string(),
                table: "events".to_string(),
            })
        );
    }

    #[test]
    fn parse_table_id_two_segments_rejected() {
        assert!(parse_table_id("analytics$events", INSTANCE).is_err());
    }

    #[test]
    fn parse_table_id_four_segments_rejected() {
        assert!(parse_table_id("a$b$c$d", INSTANCE).is_err());
    }

    #[test]
    fn parse_table_id_empty_segment_rejected() {
        assert!(parse_table_id("a$$b", INSTANCE).is_err());
        assert!(parse_table_id("a$b$", INSTANCE).is_err());
        assert!(parse_table_id("$b$c", INSTANCE).is_err());
    }
}
