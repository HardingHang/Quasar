use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use quasar_core::StoreError;
use serde::Serialize;

#[derive(Serialize)]
pub struct ProblemDetails {
    pub error: String,
    pub code: u16,
    pub detail: String,
    pub instance: String,
}

impl From<LanceError> for ProblemDetails {
    fn from(err: LanceError) -> Self {
        err.to_problem_details()
    }
}

impl IntoResponse for ProblemDetails {
    fn into_response(self) -> Response {
        let status = StatusCode::from_u16(self.code).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        (status, axum::Json(self)).into_response()
    }
}

/// Lance 协议错误类型。
pub enum LanceError {
    // --- Namespace errors ---
    NamespaceNotFound { name: String, instance: String },
    NamespaceAlreadyExists { name: String, instance: String },
    NamespaceNotEmpty { name: String, instance: String },
    // --- Table errors ---
    TableNotFound { name: String, instance: String },
    TableAlreadyExists { name: String, instance: String },
    TableNotEmpty { name: String, instance: String },
    // --- Version errors ---
    TableVersionAlreadyExists { version: i64, instance: String },
    // --- Generic errors ---
    InvalidInput { detail: String, instance: String },
    InternalError { detail: String, instance: String },
    ServiceUnavailable { detail: String, instance: String },
    Timeout { detail: String, instance: String },
}

impl LanceError {
    pub fn to_problem_details(self) -> ProblemDetails {
        match self {
            LanceError::NamespaceNotFound { name, instance } => ProblemDetails {
                error: "NamespaceNotFound".to_string(),
                code: 404,
                detail: format!("Namespace '{}' not found", name),
                instance,
            },
            LanceError::NamespaceAlreadyExists { name, instance } => ProblemDetails {
                error: "NamespaceAlreadyExists".to_string(),
                code: 409,
                detail: format!("Namespace '{}' already exists", name),
                instance,
            },
            LanceError::NamespaceNotEmpty { name, instance } => ProblemDetails {
                error: "NamespaceNotEmpty".to_string(),
                code: 409,
                detail: format!("Namespace '{}' is not empty", name),
                instance,
            },
            LanceError::TableNotFound { name, instance } => ProblemDetails {
                error: "TableNotFound".to_string(),
                code: 404,
                detail: format!("Table '{}' not found", name),
                instance,
            },
            LanceError::TableAlreadyExists { name, instance } => ProblemDetails {
                error: "TableAlreadyExists".to_string(),
                code: 409,
                detail: format!("Table '{}' already exists", name),
                instance,
            },
            LanceError::TableNotEmpty { name, instance } => ProblemDetails {
                error: "TableNotEmpty".to_string(),
                code: 409,
                detail: format!("Table '{}' is not empty", name),
                instance,
            },
            LanceError::TableVersionAlreadyExists { version, instance } => ProblemDetails {
                error: "TableVersionAlreadyExists".to_string(),
                code: 409,
                detail: format!("Version {} already exists", version),
                instance,
            },
            LanceError::InvalidInput { detail, instance } => ProblemDetails {
                error: "InvalidInput".to_string(),
                code: 400,
                detail,
                instance,
            },
            LanceError::InternalError { detail, instance } => ProblemDetails {
                error: "InternalError".to_string(),
                code: 500,
                detail,
                instance,
            },
            LanceError::ServiceUnavailable { detail, instance } => ProblemDetails {
                error: "ServiceUnavailable".to_string(),
                code: 503,
                detail,
                instance,
            },
            LanceError::Timeout { detail, instance } => ProblemDetails {
                error: "Timeout".to_string(),
                code: 504,
                detail,
                instance,
            },
        }
    }
}

impl IntoResponse for LanceError {
    fn into_response(self) -> Response {
        self.to_problem_details().into_response()
    }
}

pub fn store_error_to_lance(err: StoreError, instance: &str) -> LanceError {
    match err {
        StoreError::NotFound(msg) => LanceError::NamespaceNotFound {
            name: msg,
            instance: instance.to_string(),
        },
        StoreError::AlreadyExists(msg) => LanceError::NamespaceAlreadyExists {
            name: msg,
            instance: instance.to_string(),
        },
        StoreError::NamespaceNotEmpty { namespace } => LanceError::NamespaceNotEmpty {
            name: namespace,
            instance: instance.to_string(),
        },
        StoreError::DomainNotEmpty { domain } => LanceError::NamespaceNotEmpty {
            name: domain,
            instance: instance.to_string(),
        },
        StoreError::Conflict { msg } => LanceError::NamespaceNotEmpty {
            name: msg,
            instance: instance.to_string(),
        },
        StoreError::InvalidInput(msg) => LanceError::InvalidInput {
            detail: msg,
            instance: instance.to_string(),
        },
        StoreError::DatabaseUnavailable { .. } => LanceError::ServiceUnavailable {
            detail: "service temporarily unavailable".to_string(),
            instance: instance.to_string(),
        },
        StoreError::Timeout { operation } => LanceError::Timeout {
            detail: format!("operation '{}' timed out", operation),
            instance: instance.to_string(),
        },
        StoreError::Internal { msg, source } => {
            tracing::error!(error = ?source, %msg, "internal store error");
            LanceError::InternalError {
                detail: "An internal error occurred".to_string(),
                instance: instance.to_string(),
            }
        }
    }
}

pub fn store_error_to_lance_table(err: StoreError, instance: &str) -> LanceError {
    match err {
        StoreError::NotFound(msg) => LanceError::TableNotFound {
            name: msg,
            instance: instance.to_string(),
        },
        StoreError::AlreadyExists(msg) => LanceError::TableAlreadyExists {
            name: msg,
            instance: instance.to_string(),
        },
        StoreError::NamespaceNotEmpty { namespace } => LanceError::NamespaceNotEmpty {
            name: namespace,
            instance: instance.to_string(),
        },
        StoreError::DomainNotEmpty { domain } => LanceError::NamespaceNotEmpty {
            name: domain,
            instance: instance.to_string(),
        },
        StoreError::Conflict { msg } => LanceError::TableNotEmpty {
            name: msg,
            instance: instance.to_string(),
        },
        StoreError::InvalidInput(msg) => LanceError::InvalidInput {
            detail: msg,
            instance: instance.to_string(),
        },
        StoreError::DatabaseUnavailable { .. } => LanceError::ServiceUnavailable {
            detail: "service temporarily unavailable".to_string(),
            instance: instance.to_string(),
        },
        StoreError::Timeout { operation } => LanceError::Timeout {
            detail: format!("operation '{}' timed out", operation),
            instance: instance.to_string(),
        },
        StoreError::Internal { msg, source } => {
            tracing::error!(error = ?source, %msg, "internal store error");
            LanceError::InternalError {
                detail: "An internal error occurred".to_string(),
                instance: instance.to_string(),
            }
        }
    }
}

pub fn store_error_to_lance_version(err: StoreError, instance: &str) -> LanceError {
    match err {
        StoreError::NotFound(msg) => LanceError::TableNotFound {
            name: msg,
            instance: instance.to_string(),
        },
        StoreError::AlreadyExists(msg) => LanceError::TableAlreadyExists {
            name: msg,
            instance: instance.to_string(),
        },
        StoreError::NamespaceNotEmpty { namespace } => LanceError::NamespaceNotEmpty {
            name: namespace,
            instance: instance.to_string(),
        },
        StoreError::DomainNotEmpty { domain } => LanceError::NamespaceNotEmpty {
            name: domain,
            instance: instance.to_string(),
        },
        StoreError::Conflict { msg } => LanceError::TableNotEmpty {
            name: msg,
            instance: instance.to_string(),
        },
        StoreError::InvalidInput(msg) => LanceError::InvalidInput {
            detail: msg,
            instance: instance.to_string(),
        },
        StoreError::DatabaseUnavailable { .. } => LanceError::ServiceUnavailable {
            detail: "service temporarily unavailable".to_string(),
            instance: instance.to_string(),
        },
        StoreError::Timeout { operation } => LanceError::Timeout {
            detail: format!("operation '{}' timed out", operation),
            instance: instance.to_string(),
        },
        StoreError::Internal { msg, source } => {
            tracing::error!(error = ?source, %msg, "internal store error");
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

    #[test]
    fn test_lance_error_namespace_not_found_to_problem_details() {
        let err = LanceError::NamespaceNotFound {
            name: "foo".to_string(),
            instance: "/lance/v1/namespace/foo/describe".to_string(),
        };
        let pd = err.to_problem_details();
        assert_eq!(pd.error, "NamespaceNotFound");
        assert_eq!(pd.code, 404);
        assert_eq!(pd.detail, "Namespace 'foo' not found");
        assert_eq!(pd.instance, "/lance/v1/namespace/foo/describe");
    }

    #[test]
    fn test_lance_error_namespace_already_exists_to_problem_details() {
        let err = LanceError::NamespaceAlreadyExists {
            name: "foo".to_string(),
            instance: "/lance/v1/namespace/foo/create".to_string(),
        };
        let pd = err.to_problem_details();
        assert_eq!(pd.error, "NamespaceAlreadyExists");
        assert_eq!(pd.code, 409);
        assert_eq!(pd.detail, "Namespace 'foo' already exists");
    }

    #[test]
    fn test_lance_error_table_not_found_to_problem_details() {
        let err = LanceError::TableNotFound {
            name: "bar".to_string(),
            instance: "/lance/v1/table/ns$bar/describe".to_string(),
        };
        let pd = err.to_problem_details();
        assert_eq!(pd.error, "TableNotFound");
        assert_eq!(pd.code, 404);
        assert_eq!(pd.detail, "Table 'bar' not found");
    }

    #[test]
    fn test_lance_error_table_version_already_exists_to_problem_details() {
        let err = LanceError::TableVersionAlreadyExists {
            version: 5,
            instance: "/lance/v1/table/ns$bar/version/create".to_string(),
        };
        let pd = err.to_problem_details();
        assert_eq!(pd.error, "TableVersionAlreadyExists");
        assert_eq!(pd.code, 409);
        assert_eq!(pd.detail, "Version 5 already exists");
    }

    #[test]
    fn test_lance_error_invalid_input_to_problem_details() {
        let err = LanceError::InvalidInput {
            detail: "missing field 'name'".to_string(),
            instance: "/lance/v1/namespace/foo/create".to_string(),
        };
        let pd = err.to_problem_details();
        assert_eq!(pd.error, "InvalidInput");
        assert_eq!(pd.code, 400);
        assert_eq!(pd.detail, "missing field 'name'");
    }

    #[test]
    fn test_lance_error_internal_error_to_problem_details() {
        let err = LanceError::InternalError {
            detail: "db connection lost".to_string(),
            instance: "/lance/v1/namespace/foo/list".to_string(),
        };
        let pd = err.to_problem_details();
        assert_eq!(pd.error, "InternalError");
        assert_eq!(pd.code, 500);
        assert_eq!(pd.detail, "db connection lost");
    }

    #[test]
    fn test_lance_error_service_unavailable_to_problem_details() {
        let err = LanceError::ServiceUnavailable {
            detail: "service temporarily unavailable".to_string(),
            instance: "/lance/v1/namespace/foo/list".to_string(),
        };
        let pd = err.to_problem_details();
        assert_eq!(pd.error, "ServiceUnavailable");
        assert_eq!(pd.code, 503);
        assert_eq!(pd.detail, "service temporarily unavailable");
    }

    #[test]
    fn test_lance_error_timeout_to_problem_details() {
        let err = LanceError::Timeout {
            detail: "operation 'list_namespaces' timed out".to_string(),
            instance: "/lance/v1/namespace/foo/list".to_string(),
        };
        let pd = err.to_problem_details();
        assert_eq!(pd.error, "Timeout");
        assert_eq!(pd.code, 504);
        assert_eq!(pd.detail, "operation 'list_namespaces' timed out");
    }

    #[test]
    fn test_problem_details_serde() {
        let pd = ProblemDetails {
            error: "NotFound".to_string(),
            code: 404,
            detail: "not here".to_string(),
            instance: "/test".to_string(),
        };
        let json = serde_json::to_string(&pd).unwrap();
        let decoded: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded["error"], "NotFound");
        assert_eq!(decoded["code"], 404);
        assert_eq!(decoded["detail"], "not here");
        assert_eq!(decoded["instance"], "/test");
    }

    #[test]
    fn test_store_error_to_lance_mapping() {
        assert!(matches!(
            store_error_to_lance(StoreError::NotFound("foo".into()), "/test"),
            LanceError::NamespaceNotFound { name, .. } if name == "foo"
        ));
        assert!(matches!(
            store_error_to_lance(StoreError::AlreadyExists("foo".into()), "/test"),
            LanceError::NamespaceAlreadyExists { name, .. } if name == "foo"
        ));
        assert!(matches!(
            store_error_to_lance(StoreError::Conflict { msg: "foo".into() }, "/test"),
            LanceError::NamespaceNotEmpty { name, .. } if name == "foo"
        ));
        assert!(matches!(
            store_error_to_lance(StoreError::InvalidInput("bad".into()), "/test"),
            LanceError::InvalidInput { detail, .. } if detail == "bad"
        ));
        assert!(matches!(
            store_error_to_lance(StoreError::Internal { msg: "oops".into(), source: None }, "/test"),
            LanceError::InternalError { detail, .. } if detail == "An internal error occurred"
        ));
        assert!(matches!(
            store_error_to_lance(StoreError::DatabaseUnavailable { source: None }, "/test"),
            LanceError::ServiceUnavailable { detail, .. } if detail == "service temporarily unavailable"
        ));
        assert!(matches!(
            store_error_to_lance(StoreError::Timeout { operation: "list".into() }, "/test"),
            LanceError::Timeout { detail, .. } if detail.contains("list")
        ));
    }

    #[test]
    fn test_store_error_to_lance_table_mapping() {
        assert!(matches!(
            store_error_to_lance_table(StoreError::NotFound("bar".into()), "/test"),
            LanceError::TableNotFound { name, .. } if name == "bar"
        ));
        assert!(matches!(
            store_error_to_lance_table(StoreError::AlreadyExists("bar".into()), "/test"),
            LanceError::TableAlreadyExists { name, .. } if name == "bar"
        ));
        assert!(matches!(
            store_error_to_lance_table(StoreError::DatabaseUnavailable { source: None }, "/test"),
            LanceError::ServiceUnavailable { detail, .. } if detail == "service temporarily unavailable"
        ));
        assert!(matches!(
            store_error_to_lance_table(StoreError::Timeout { operation: "describe".into() }, "/test"),
            LanceError::Timeout { detail, .. } if detail.contains("describe")
        ));
    }

    #[test]
    fn test_store_error_to_lance_version_already_exists_maps_to_table_exists() {
        // V3: storage CAS conflicts surface as `Conflict`, not `AlreadyExists`.
        // The adapter no longer parses the message string to invent a version
        // number; any `AlreadyExists` here is treated as a name collision.
        let err = store_error_to_lance_version(
            StoreError::AlreadyExists("table bar already exists".into()),
            "/test",
        );
        assert!(matches!(
            err,
            LanceError::TableAlreadyExists { name, .. } if name == "table bar already exists"
        ));
    }

    #[test]
    fn test_store_error_to_lance_version_not_found() {
        assert!(matches!(
            store_error_to_lance_version(StoreError::NotFound("bar".into()), "/test"),
            LanceError::TableNotFound { name, .. } if name == "bar"
        ));
    }

    #[test]
    fn test_store_error_to_lance_version_conflict() {
        assert!(matches!(
            store_error_to_lance_version(StoreError::Conflict { msg: "not empty".into() }, "/test"),
            LanceError::TableNotEmpty { name, .. } if name == "not empty"
        ));
    }

    #[test]
    fn test_store_error_to_lance_version_invalid_input() {
        assert!(matches!(
            store_error_to_lance_version(StoreError::InvalidInput("bad".into()), "/test"),
            LanceError::InvalidInput { detail, .. } if detail == "bad"
        ));
    }

    #[test]
    fn test_store_error_to_lance_version_internal() {
        assert!(matches!(
            store_error_to_lance_version(StoreError::Internal { msg: "oops".into(), source: None }, "/test"),
            LanceError::InternalError { detail, .. } if detail == "An internal error occurred"
        ));
    }
}
