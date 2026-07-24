//! RFC-7807 Problem Details error responses for the Lance REST Namespace
//! adapter (DESIGN §5.2 / §7.6), extended with `code` and `request_id`.
//!
//! `code` carries the Lance error name (e.g. `NamespaceNotFound`,
//! `TableAlreadyExists`) following the upstream spec's error-code style.
//! `Transient` and `Internal` contexts are sanitized per DESIGN §7.3:
//! they are logged, never returned to the client.

use axum::{
    http::{header, StatusCode},
    response::{IntoResponse, Response},
};
use quasar_core::CatalogError;
use serde::Serialize;

/// RFC-7807 Problem Details body, extended with `code` and `request_id`.
#[derive(Debug, Serialize)]
pub struct ProblemDetails {
    #[serde(rename = "type")]
    pub problem_type: String,
    pub title: String,
    pub status: u16,
    pub detail: String,
    pub instance: String,
    pub code: String,
    pub request_id: String,
}

impl IntoResponse for ProblemDetails {
    fn into_response(self) -> Response {
        let status = StatusCode::from_u16(self.status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        let body = serde_json::to_string(&self).unwrap_or_else(|_| {
            r#"{"type":"https://quasar.io/errors/internal-error","title":"Internal Error","status":500,"detail":"failed to serialize error response","instance":"","code":"InternalError","request_id":""}"#.to_string()
        });
        (
            status,
            [(header::CONTENT_TYPE, "application/problem+json")],
            body,
        )
            .into_response()
    }
}

/// Lance protocol error type.
pub enum LanceError {
    // --- Namespace errors ---
    NamespaceNotFound { name: String, instance: String },
    NamespaceAlreadyExists { name: String, instance: String },
    NamespaceNotEmpty { name: String, instance: String },
    // --- Table errors ---
    TableNotFound { name: String, instance: String },
    TableAlreadyExists { name: String, instance: String },
    TableNotEmpty { name: String, instance: String },
    // --- Generic errors ---
    InvalidInput { detail: String, instance: String },
    InternalError { detail: String, instance: String },
    ServiceUnavailable { detail: String, instance: String },
    Timeout { detail: String, instance: String },
}

impl LanceError {
    fn status(&self) -> StatusCode {
        match self {
            Self::NamespaceNotFound { .. } | Self::TableNotFound { .. } => StatusCode::NOT_FOUND,
            Self::NamespaceAlreadyExists { .. }
            | Self::NamespaceNotEmpty { .. }
            | Self::TableAlreadyExists { .. }
            | Self::TableNotEmpty { .. } => StatusCode::CONFLICT,
            Self::InvalidInput { .. } => StatusCode::BAD_REQUEST,
            Self::InternalError { .. } => StatusCode::INTERNAL_SERVER_ERROR,
            Self::ServiceUnavailable { .. } => StatusCode::SERVICE_UNAVAILABLE,
            Self::Timeout { .. } => StatusCode::GATEWAY_TIMEOUT,
        }
    }

    /// Lance error name carried by the `code` extension field.
    fn code_name(&self) -> &'static str {
        match self {
            Self::NamespaceNotFound { .. } => "NamespaceNotFound",
            Self::NamespaceAlreadyExists { .. } => "NamespaceAlreadyExists",
            Self::NamespaceNotEmpty { .. } => "NamespaceNotEmpty",
            Self::TableNotFound { .. } => "TableNotFound",
            Self::TableAlreadyExists { .. } => "TableAlreadyExists",
            Self::TableNotEmpty { .. } => "TableNotEmpty",
            Self::InvalidInput { .. } => "InvalidInput",
            Self::InternalError { .. } => "InternalError",
            Self::ServiceUnavailable { .. } => "ServiceUnavailable",
            Self::Timeout { .. } => "Timeout",
        }
    }

    /// Human-readable title for the problem type.
    fn title(&self) -> &'static str {
        match self {
            Self::NamespaceNotFound { .. } => "Namespace Not Found",
            Self::NamespaceAlreadyExists { .. } => "Namespace Already Exists",
            Self::NamespaceNotEmpty { .. } => "Namespace Not Empty",
            Self::TableNotFound { .. } => "Table Not Found",
            Self::TableAlreadyExists { .. } => "Table Already Exists",
            Self::TableNotEmpty { .. } => "Table Not Empty",
            Self::InvalidInput { .. } => "Invalid Input",
            Self::InternalError { .. } => "Internal Error",
            Self::ServiceUnavailable { .. } => "Service Unavailable",
            Self::Timeout { .. } => "Timeout",
        }
    }

    /// Slug used in the stable `type` URI: `https://quasar.io/errors/<slug>`.
    fn slug(&self) -> &'static str {
        match self {
            Self::NamespaceNotFound { .. } => "namespace-not-found",
            Self::NamespaceAlreadyExists { .. } => "namespace-already-exists",
            Self::NamespaceNotEmpty { .. } => "namespace-not-empty",
            Self::TableNotFound { .. } => "table-not-found",
            Self::TableAlreadyExists { .. } => "table-already-exists",
            Self::TableNotEmpty { .. } => "table-not-empty",
            Self::InvalidInput { .. } => "invalid-input",
            Self::InternalError { .. } => "internal-error",
            Self::ServiceUnavailable { .. } => "service-unavailable",
            Self::Timeout { .. } => "timeout",
        }
    }

    fn detail(&self) -> String {
        match self {
            Self::NamespaceNotFound { name, .. } => format!("Namespace '{}' not found", name),
            Self::NamespaceAlreadyExists { name, .. } => {
                format!("Namespace '{}' already exists", name)
            }
            Self::NamespaceNotEmpty { name, .. } => format!("Namespace '{}' is not empty", name),
            Self::TableNotFound { name, .. } => format!("Table '{}' not found", name),
            Self::TableAlreadyExists { name, .. } => format!("Table '{}' already exists", name),
            Self::TableNotEmpty { name, .. } => format!("Table '{}' is not empty", name),
            Self::InvalidInput { detail, .. }
            | Self::InternalError { detail, .. }
            | Self::ServiceUnavailable { detail, .. }
            | Self::Timeout { detail, .. } => detail.clone(),
        }
    }

    fn instance(&self) -> &str {
        match self {
            Self::NamespaceNotFound { instance, .. }
            | Self::NamespaceAlreadyExists { instance, .. }
            | Self::NamespaceNotEmpty { instance, .. }
            | Self::TableNotFound { instance, .. }
            | Self::TableAlreadyExists { instance, .. }
            | Self::TableNotEmpty { instance, .. }
            | Self::InvalidInput { instance, .. }
            | Self::InternalError { instance, .. }
            | Self::ServiceUnavailable { instance, .. }
            | Self::Timeout { instance, .. } => instance,
        }
    }

    pub fn to_problem_details(&self, request_id: &str) -> ProblemDetails {
        ProblemDetails {
            problem_type: format!("https://quasar.io/errors/{}", self.slug()),
            title: self.title().to_string(),
            status: self.status().as_u16(),
            detail: self.detail(),
            instance: self.instance().to_string(),
            code: self.code_name().to_string(),
            request_id: request_id.to_string(),
        }
    }
}

/// Map a `CatalogError` to a `LanceError` in the Namespace endpoint
/// context (DESIGN §7.5): NotFound reads as NamespaceNotFound, a Conflict
/// on write/delete reads as NamespaceNotEmpty.
pub fn catalog_error_to_lance(err: CatalogError, instance: &str) -> LanceError {
    match err {
        CatalogError::NotFound(msg) => LanceError::NamespaceNotFound {
            name: msg,
            instance: instance.to_string(),
        },
        CatalogError::AlreadyExists(msg) => LanceError::NamespaceAlreadyExists {
            name: msg,
            instance: instance.to_string(),
        },
        CatalogError::Conflict(msg) => LanceError::NamespaceNotEmpty {
            name: msg,
            instance: instance.to_string(),
        },
        CatalogError::Validation(msg) => LanceError::InvalidInput {
            detail: msg,
            instance: instance.to_string(),
        },
        CatalogError::Transient(msg) => {
            tracing::warn!(error = %msg, "transient store error");
            LanceError::ServiceUnavailable {
                detail: "service temporarily unavailable".to_string(),
                instance: instance.to_string(),
            }
        }
        CatalogError::Internal(msg) => {
            tracing::error!(error = %msg, "internal store error");
            LanceError::InternalError {
                detail: "An internal error occurred".to_string(),
                instance: instance.to_string(),
            }
        }
    }
}

/// Map a `CatalogError` to a `LanceError` in the Table/Version endpoint
/// context (DESIGN §7.5): NotFound reads as TableNotFound, AlreadyExists
/// (including a duplicate mirrored version key) as TableAlreadyExists.
pub fn catalog_error_to_lance_table(err: CatalogError, instance: &str) -> LanceError {
    match err {
        CatalogError::NotFound(msg) => LanceError::TableNotFound {
            name: msg,
            instance: instance.to_string(),
        },
        CatalogError::AlreadyExists(msg) => LanceError::TableAlreadyExists {
            name: msg,
            instance: instance.to_string(),
        },
        CatalogError::Conflict(msg) => LanceError::TableNotEmpty {
            name: msg,
            instance: instance.to_string(),
        },
        CatalogError::Validation(msg) => LanceError::InvalidInput {
            detail: msg,
            instance: instance.to_string(),
        },
        CatalogError::Transient(msg) => {
            tracing::warn!(error = %msg, "transient store error");
            LanceError::ServiceUnavailable {
                detail: "service temporarily unavailable".to_string(),
                instance: instance.to_string(),
            }
        }
        CatalogError::Internal(msg) => {
            tracing::error!(error = %msg, "internal store error");
            LanceError::InternalError {
                detail: "An internal error occurred".to_string(),
                instance: instance.to_string(),
            }
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    const REQUEST_ID: &str = "req-test-1";

    #[test]
    fn namespace_not_found_to_problem_details() {
        let err = LanceError::NamespaceNotFound {
            name: "foo".to_string(),
            instance: "/lance/v1/namespace/foo/describe".to_string(),
        };
        let pd = err.to_problem_details(REQUEST_ID);
        assert_eq!(
            pd.problem_type,
            "https://quasar.io/errors/namespace-not-found"
        );
        assert_eq!(pd.title, "Namespace Not Found");
        assert_eq!(pd.status, 404);
        assert_eq!(pd.detail, "Namespace 'foo' not found");
        assert_eq!(pd.instance, "/lance/v1/namespace/foo/describe");
        assert_eq!(pd.code, "NamespaceNotFound");
        assert_eq!(pd.request_id, REQUEST_ID);
    }

    #[test]
    fn namespace_already_exists_to_problem_details() {
        let err = LanceError::NamespaceAlreadyExists {
            name: "foo".to_string(),
            instance: "/lance/v1/namespace/foo/create".to_string(),
        };
        let pd = err.to_problem_details(REQUEST_ID);
        assert_eq!(pd.status, 409);
        assert_eq!(pd.code, "NamespaceAlreadyExists");
        assert_eq!(pd.detail, "Namespace 'foo' already exists");
    }

    #[test]
    fn table_not_found_to_problem_details() {
        let err = LanceError::TableNotFound {
            name: "bar".to_string(),
            instance: "/lance/v1/table/ns$bar/describe".to_string(),
        };
        let pd = err.to_problem_details(REQUEST_ID);
        assert_eq!(pd.status, 404);
        assert_eq!(pd.code, "TableNotFound");
        assert_eq!(pd.detail, "Table 'bar' not found");
    }

    #[test]
    fn invalid_input_to_problem_details() {
        let err = LanceError::InvalidInput {
            detail: "missing field 'name'".to_string(),
            instance: "/lance/v1/namespace/foo/create".to_string(),
        };
        let pd = err.to_problem_details(REQUEST_ID);
        assert_eq!(pd.status, 400);
        assert_eq!(pd.code, "InvalidInput");
        assert_eq!(pd.detail, "missing field 'name'");
    }

    #[test]
    fn internal_error_to_problem_details() {
        let err = LanceError::InternalError {
            detail: "An internal error occurred".to_string(),
            instance: "/lance/v1/namespace/foo/list".to_string(),
        };
        let pd = err.to_problem_details(REQUEST_ID);
        assert_eq!(pd.status, 500);
        assert_eq!(pd.code, "InternalError");
    }

    #[test]
    fn service_unavailable_to_problem_details() {
        let err = LanceError::ServiceUnavailable {
            detail: "service temporarily unavailable".to_string(),
            instance: "/lance/v1/namespace/foo/list".to_string(),
        };
        let pd = err.to_problem_details(REQUEST_ID);
        assert_eq!(pd.status, 503);
        assert_eq!(pd.code, "ServiceUnavailable");
    }

    #[test]
    fn timeout_to_problem_details() {
        let err = LanceError::Timeout {
            detail: "operation 'list_namespaces' timed out".to_string(),
            instance: "/lance/v1/namespace/foo/list".to_string(),
        };
        let pd = err.to_problem_details(REQUEST_ID);
        assert_eq!(pd.status, 504);
        assert_eq!(pd.code, "Timeout");
    }

    #[test]
    fn problem_details_serializes_rfc7807_fields() {
        let err = LanceError::TableNotEmpty {
            name: "bar".to_string(),
            instance: "/test".to_string(),
        };
        let pd = err.to_problem_details(REQUEST_ID);
        let json = serde_json::to_string(&pd).unwrap();
        let decoded: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded["type"], "https://quasar.io/errors/table-not-empty");
        assert_eq!(decoded["title"], "Table Not Empty");
        assert_eq!(decoded["status"], 409);
        assert_eq!(decoded["detail"], "Table 'bar' is not empty");
        assert_eq!(decoded["instance"], "/test");
        assert_eq!(decoded["code"], "TableNotEmpty");
        assert_eq!(decoded["request_id"], REQUEST_ID);
    }

    #[test]
    fn catalog_error_namespace_context_mapping() {
        assert!(matches!(
            catalog_error_to_lance(CatalogError::NotFound("foo".into()), "/test"),
            LanceError::NamespaceNotFound { name, .. } if name == "foo"
        ));
        assert!(matches!(
            catalog_error_to_lance(CatalogError::AlreadyExists("foo".into()), "/test"),
            LanceError::NamespaceAlreadyExists { name, .. } if name == "foo"
        ));
        assert!(matches!(
            catalog_error_to_lance(CatalogError::Conflict("foo".into()), "/test"),
            LanceError::NamespaceNotEmpty { name, .. } if name == "foo"
        ));
        assert!(matches!(
            catalog_error_to_lance(CatalogError::Validation("bad".into()), "/test"),
            LanceError::InvalidInput { detail, .. } if detail == "bad"
        ));
        assert!(matches!(
            catalog_error_to_lance(CatalogError::Transient("oops".into()), "/test"),
            LanceError::ServiceUnavailable { .. }
        ));
        assert!(matches!(
            catalog_error_to_lance(CatalogError::Internal("oops".into()), "/test"),
            LanceError::InternalError { detail, .. } if detail == "An internal error occurred"
        ));
    }

    #[test]
    fn catalog_error_table_context_mapping() {
        assert!(matches!(
            catalog_error_to_lance_table(CatalogError::NotFound("bar".into()), "/test"),
            LanceError::TableNotFound { name, .. } if name == "bar"
        ));
        assert!(matches!(
            catalog_error_to_lance_table(CatalogError::AlreadyExists("bar".into()), "/test"),
            LanceError::TableAlreadyExists { name, .. } if name == "bar"
        ));
        assert!(matches!(
            catalog_error_to_lance_table(CatalogError::Conflict("not empty".into()), "/test"),
            LanceError::TableNotEmpty { name, .. } if name == "not empty"
        ));
        assert!(matches!(
            catalog_error_to_lance_table(CatalogError::Validation("bad".into()), "/test"),
            LanceError::InvalidInput { detail, .. } if detail == "bad"
        ));
        assert!(matches!(
            catalog_error_to_lance_table(CatalogError::Internal("oops".into()), "/test"),
            LanceError::InternalError { detail, .. } if detail == "An internal error occurred"
        ));
    }

    #[test]
    fn internal_and_transient_details_are_sanitized() {
        let err = catalog_error_to_lance(
            CatalogError::Internal("db password is hunter2".into()),
            "/test",
        );
        assert!(!err.detail().contains("hunter2"));

        let err = catalog_error_to_lance_table(
            CatalogError::Transient("pool at postgres://secret".into()),
            "/test",
        );
        assert!(!err.detail().contains("secret"));
    }
}
