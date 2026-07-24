use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};
use quasar_core::CatalogError;
use serde::Serialize;

#[derive(Serialize)]
pub struct ErrorResponse {
    pub error: ErrorBody,
}

#[derive(Serialize)]
pub struct ErrorBody {
    pub message: String,
    #[serde(rename = "type")]
    pub error_type: String,
    pub code: u16,
}

impl IntoResponse for ErrorResponse {
    fn into_response(self) -> Response {
        let status =
            StatusCode::from_u16(self.error.code).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        (status, axum::Json(self)).into_response()
    }
}

#[derive(Debug)]
pub enum IcebergError {
    NoSuchNamespaceException { message: String },
    NamespaceAlreadyExistsException { message: String },
    NamespaceNotEmptyException { message: String },
    NoSuchTableException { message: String },
    TableAlreadyExistsException { message: String },
    BadRequestException { message: String },
    CommitFailedException { message: String },
    InternalServerError { message: String },
    ServiceUnavailableException { message: String },
    TimeoutException { message: String },
    MetadataNotFoundException { message: String },
    NotImplementedException { message: String },
    NoSuchWarehouseException { message: String },
    NoSuchViewException { message: String },
    ViewAlreadyExistsException { message: String },
    AlreadyExistsException { message: String },
}

impl IcebergError {
    pub fn to_error_response(&self) -> ErrorResponse {
        let (message, error_type, code) = match self {
            IcebergError::NoSuchNamespaceException { message } => {
                (message.clone(), "NoSuchNamespaceException".to_string(), 404)
            }
            IcebergError::NamespaceAlreadyExistsException { message } => (
                message.clone(),
                "NamespaceAlreadyExistsException".to_string(),
                409,
            ),
            IcebergError::NamespaceNotEmptyException { message } => (
                message.clone(),
                "NamespaceNotEmptyException".to_string(),
                409,
            ),
            IcebergError::NoSuchTableException { message } => {
                (message.clone(), "NoSuchTableException".to_string(), 404)
            }
            IcebergError::TableAlreadyExistsException { message } => (
                message.clone(),
                "TableAlreadyExistsException".to_string(),
                409,
            ),
            IcebergError::BadRequestException { message } => {
                (message.clone(), "BadRequestException".to_string(), 400)
            }
            IcebergError::CommitFailedException { message } => {
                (message.clone(), "CommitFailedException".to_string(), 409)
            }
            IcebergError::InternalServerError { message } => {
                (message.clone(), "InternalServerError".to_string(), 500)
            }
            IcebergError::ServiceUnavailableException { message } => (
                message.clone(),
                "ServiceUnavailableException".to_string(),
                503,
            ),
            IcebergError::TimeoutException { message } => {
                (message.clone(), "TimeoutException".to_string(), 504)
            }
            IcebergError::MetadataNotFoundException { message } => (
                message.clone(),
                "MetadataNotFoundException".to_string(),
                404,
            ),
            IcebergError::NotImplementedException { message } => {
                (message.clone(), "NotImplementedException".to_string(), 501)
            }
            IcebergError::NoSuchWarehouseException { message } => {
                (message.clone(), "NoSuchWarehouseException".to_string(), 404)
            }
            IcebergError::NoSuchViewException { message } => {
                (message.clone(), "NoSuchViewException".to_string(), 404)
            }
            IcebergError::ViewAlreadyExistsException { message } => (
                message.clone(),
                "ViewAlreadyExistsException".to_string(),
                409,
            ),
            IcebergError::AlreadyExistsException { message } => {
                (message.clone(), "AlreadyExistsException".to_string(), 409)
            }
        };

        ErrorResponse {
            error: ErrorBody {
                message,
                error_type,
                code,
            },
        }
    }
}

impl IntoResponse for IcebergError {
    fn into_response(self) -> Response {
        self.to_error_response().into_response()
    }
}

/// Shared tail of the CatalogError mapping: variants whose mapping does
/// not depend on the endpoint context. `Transient` and `Internal` contexts
/// are logged and sanitized before reaching the client (DESIGN §7.3).
fn map_common(err: CatalogError) -> Option<IcebergError> {
    match err {
        CatalogError::Validation(msg) => Some(IcebergError::BadRequestException { message: msg }),
        CatalogError::Transient(msg) => {
            tracing::warn!(error = %msg, "transient store error");
            Some(IcebergError::ServiceUnavailableException {
                message: "service temporarily unavailable".to_string(),
            })
        }
        CatalogError::Internal(msg) => {
            tracing::error!(error = %msg, "internal store error");
            Some(IcebergError::InternalServerError {
                message: "An internal error occurred".to_string(),
            })
        }
        _ => None,
    }
}

/// Map a `CatalogError` to an `IcebergError` in the Namespace endpoint
/// context (DESIGN §7.5).
pub fn catalog_error_to_iceberg_namespace(err: CatalogError) -> IcebergError {
    match err {
        CatalogError::NotFound(msg) => IcebergError::NoSuchNamespaceException { message: msg },
        CatalogError::AlreadyExists(msg) | CatalogError::Conflict(msg) => {
            IcebergError::NamespaceAlreadyExistsException { message: msg }
        }
        other => map_common(other).unwrap_or(IcebergError::InternalServerError {
            message: "An internal error occurred".to_string(),
        }),
    }
}

/// Map a `CatalogError` to an `IcebergError` in the Table endpoint context:
/// a Conflict (CAS failure) reads as CommitFailedException.
pub fn catalog_error_to_iceberg_table(err: CatalogError) -> IcebergError {
    match err {
        CatalogError::NotFound(msg) => IcebergError::NoSuchTableException { message: msg },
        CatalogError::AlreadyExists(msg) => {
            IcebergError::TableAlreadyExistsException { message: msg }
        }
        CatalogError::Conflict(msg) => IcebergError::CommitFailedException { message: msg },
        other => map_common(other).unwrap_or(IcebergError::InternalServerError {
            message: "An internal error occurred".to_string(),
        }),
    }
}

/// Map a `CatalogError` to an `IcebergError` in the View endpoint context:
/// a Conflict (CAS failure) reads as CommitFailedException.
pub fn catalog_error_to_iceberg_view(err: CatalogError) -> IcebergError {
    match err {
        CatalogError::NotFound(msg) => IcebergError::NoSuchViewException { message: msg },
        CatalogError::AlreadyExists(msg) => {
            IcebergError::ViewAlreadyExistsException { message: msg }
        }
        CatalogError::Conflict(msg) => IcebergError::CommitFailedException { message: msg },
        other => map_common(other).unwrap_or(IcebergError::InternalServerError {
            message: "An internal error occurred".to_string(),
        }),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn test_iceberg_error_no_such_namespace_to_response() {
        let err = IcebergError::NoSuchNamespaceException {
            message: "Namespace 'foo' not found".to_string(),
        };
        let resp = err.to_error_response();
        assert_eq!(resp.error.error_type, "NoSuchNamespaceException");
        assert_eq!(resp.error.code, 404);
        assert_eq!(resp.error.message, "Namespace 'foo' not found");
    }

    #[test]
    fn test_iceberg_error_namespace_already_exists_to_response() {
        let err = IcebergError::NamespaceAlreadyExistsException {
            message: "Namespace 'foo' already exists".to_string(),
        };
        let resp = err.to_error_response();
        assert_eq!(resp.error.error_type, "NamespaceAlreadyExistsException");
        assert_eq!(resp.error.code, 409);
    }

    #[test]
    fn test_iceberg_error_no_such_table_to_response() {
        let err = IcebergError::NoSuchTableException {
            message: "Table 'foo.bar' not found".to_string(),
        };
        let resp = err.to_error_response();
        assert_eq!(resp.error.error_type, "NoSuchTableException");
        assert_eq!(resp.error.code, 404);
    }

    #[test]
    fn test_iceberg_error_table_already_exists_to_response() {
        let err = IcebergError::TableAlreadyExistsException {
            message: "Table 'foo.bar' already exists".to_string(),
        };
        let resp = err.to_error_response();
        assert_eq!(resp.error.error_type, "TableAlreadyExistsException");
        assert_eq!(resp.error.code, 409);
    }

    #[test]
    fn test_iceberg_error_bad_request_to_response() {
        let err = IcebergError::BadRequestException {
            message: "Invalid input".to_string(),
        };
        let resp = err.to_error_response();
        assert_eq!(resp.error.error_type, "BadRequestException");
        assert_eq!(resp.error.code, 400);
    }

    #[test]
    fn test_iceberg_error_commit_failed_to_response() {
        let err = IcebergError::CommitFailedException {
            message: "Commit conflict".to_string(),
        };
        let resp = err.to_error_response();
        assert_eq!(resp.error.error_type, "CommitFailedException");
        assert_eq!(resp.error.code, 409);
    }

    #[test]
    fn test_iceberg_error_internal_to_response() {
        let err = IcebergError::InternalServerError {
            message: "Database error".to_string(),
        };
        let resp = err.to_error_response();
        assert_eq!(resp.error.error_type, "InternalServerError");
        assert_eq!(resp.error.code, 500);
    }

    #[test]
    fn test_iceberg_error_metadata_not_found_to_response() {
        let err = IcebergError::MetadataNotFoundException {
            message: "metadata.json not found at s3://bucket/path".to_string(),
        };
        let resp = err.to_error_response();
        assert_eq!(resp.error.error_type, "MetadataNotFoundException");
        assert_eq!(resp.error.code, 404);
        assert_eq!(
            resp.error.message,
            "metadata.json not found at s3://bucket/path"
        );
    }

    #[test]
    fn test_iceberg_error_service_unavailable_to_response() {
        let err = IcebergError::ServiceUnavailableException {
            message: "service temporarily unavailable".to_string(),
        };
        let resp = err.to_error_response();
        assert_eq!(resp.error.error_type, "ServiceUnavailableException");
        assert_eq!(resp.error.code, 503);
        assert_eq!(resp.error.message, "service temporarily unavailable");
    }

    #[test]
    fn test_iceberg_error_timeout_to_response() {
        let err = IcebergError::TimeoutException {
            message: "operation 'list_tables' timed out".to_string(),
        };
        let resp = err.to_error_response();
        assert_eq!(resp.error.error_type, "TimeoutException");
        assert_eq!(resp.error.code, 504);
        assert_eq!(resp.error.message, "operation 'list_tables' timed out");
    }

    #[test]
    fn test_iceberg_error_not_implemented_to_response() {
        let err = IcebergError::NotImplementedException {
            message: "cross-namespace rename is not supported".to_string(),
        };
        let resp = err.to_error_response();
        assert_eq!(resp.error.error_type, "NotImplementedException");
        assert_eq!(resp.error.code, 501);
    }

    #[test]
    fn test_error_response_serde() {
        let resp = ErrorResponse {
            error: ErrorBody {
                message: "test message".to_string(),
                error_type: "TestException".to_string(),
                code: 404,
            },
        };
        let json = serde_json::to_string(&resp).unwrap();
        let decoded: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded["error"]["message"], "test message");
        assert_eq!(decoded["error"]["type"], "TestException");
        assert_eq!(decoded["error"]["code"], 404);
    }

    #[test]
    fn test_catalog_error_to_iceberg_namespace_mapping() {
        assert!(matches!(
            catalog_error_to_iceberg_namespace(CatalogError::NotFound("foo".into())),
            IcebergError::NoSuchNamespaceException { message } if message == "foo"
        ));
        assert!(matches!(
            catalog_error_to_iceberg_namespace(CatalogError::AlreadyExists("foo".into())),
            IcebergError::NamespaceAlreadyExistsException { message } if message == "foo"
        ));
        assert!(matches!(
            catalog_error_to_iceberg_namespace(CatalogError::Conflict("foo".into())),
            IcebergError::NamespaceAlreadyExistsException { message } if message == "foo"
        ));
        assert!(matches!(
            catalog_error_to_iceberg_namespace(CatalogError::Validation("bad".into())),
            IcebergError::BadRequestException { message } if message == "bad"
        ));
        assert!(matches!(
            catalog_error_to_iceberg_namespace(CatalogError::Internal("oops".into())),
            IcebergError::InternalServerError { message } if message == "An internal error occurred"
        ));
        assert!(matches!(
            catalog_error_to_iceberg_namespace(CatalogError::Transient("oops".into())),
            IcebergError::ServiceUnavailableException { message } if message == "service temporarily unavailable"
        ));
    }

    #[test]
    fn test_catalog_error_to_iceberg_table_mapping() {
        assert!(matches!(
            catalog_error_to_iceberg_table(CatalogError::NotFound("bar".into())),
            IcebergError::NoSuchTableException { message } if message == "bar"
        ));
        assert!(matches!(
            catalog_error_to_iceberg_table(CatalogError::AlreadyExists("bar".into())),
            IcebergError::TableAlreadyExistsException { message } if message == "bar"
        ));
        assert!(matches!(
            catalog_error_to_iceberg_table(CatalogError::Conflict("conflict".into())),
            IcebergError::CommitFailedException { message } if message == "conflict"
        ));
        assert!(matches!(
            catalog_error_to_iceberg_table(CatalogError::Validation("bad".into())),
            IcebergError::BadRequestException { message } if message == "bad"
        ));
        assert!(matches!(
            catalog_error_to_iceberg_table(CatalogError::Internal("oops".into())),
            IcebergError::InternalServerError { message } if message == "An internal error occurred"
        ));
        assert!(matches!(
            catalog_error_to_iceberg_table(CatalogError::Transient("oops".into())),
            IcebergError::ServiceUnavailableException { message } if message == "service temporarily unavailable"
        ));
    }

    #[test]
    fn test_catalog_error_to_iceberg_view_mapping() {
        assert!(matches!(
            catalog_error_to_iceberg_view(CatalogError::NotFound("bar".into())),
            IcebergError::NoSuchViewException { message } if message == "bar"
        ));
        assert!(matches!(
            catalog_error_to_iceberg_view(CatalogError::AlreadyExists("bar".into())),
            IcebergError::ViewAlreadyExistsException { message } if message == "bar"
        ));
        assert!(matches!(
            catalog_error_to_iceberg_view(CatalogError::Conflict("conflict".into())),
            IcebergError::CommitFailedException { message } if message == "conflict"
        ));
        assert!(matches!(
            catalog_error_to_iceberg_view(CatalogError::Validation("bad".into())),
            IcebergError::BadRequestException { message } if message == "bad"
        ));
        assert!(matches!(
            catalog_error_to_iceberg_view(CatalogError::Internal("oops".into())),
            IcebergError::InternalServerError { message } if message == "An internal error occurred"
        ));
        assert!(matches!(
            catalog_error_to_iceberg_view(CatalogError::Transient("oops".into())),
            IcebergError::ServiceUnavailableException { message } if message == "service temporarily unavailable"
        ));
    }

    #[test]
    fn test_view_error_variants_to_response() {
        let err = IcebergError::NoSuchViewException {
            message: "View not found".to_string(),
        };
        let resp = err.to_error_response();
        assert_eq!(resp.error.error_type, "NoSuchViewException");
        assert_eq!(resp.error.code, 404);

        let err = IcebergError::ViewAlreadyExistsException {
            message: "View exists".to_string(),
        };
        let resp = err.to_error_response();
        assert_eq!(resp.error.error_type, "ViewAlreadyExistsException");
        assert_eq!(resp.error.code, 409);

        let err = IcebergError::AlreadyExistsException {
            message: "Already exists".to_string(),
        };
        let resp = err.to_error_response();
        assert_eq!(resp.error.error_type, "AlreadyExistsException");
        assert_eq!(resp.error.code, 409);
    }

    #[test]
    fn internal_and_transient_messages_are_sanitized() {
        let err =
            catalog_error_to_iceberg_table(CatalogError::Internal("db password is hunter2".into()));
        let resp = err.to_error_response();
        assert!(!resp.error.message.contains("hunter2"));

        let err = catalog_error_to_iceberg_table(CatalogError::Transient(
            "pool at postgres://secret".into(),
        ));
        let resp = err.to_error_response();
        assert!(!resp.error.message.contains("secret"));
    }
}
