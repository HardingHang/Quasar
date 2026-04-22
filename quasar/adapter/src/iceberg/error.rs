use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};
use quasar_core::StoreError;
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
        let status = StatusCode::from_u16(self.error.code)
            .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
        (status, axum::Json(self)).into_response()
    }
}

#[derive(Debug)]
pub enum IcebergError {
    NoSuchNamespaceException { message: String },
    NamespaceAlreadyExistsException { message: String },
    NoSuchTableException { message: String },
    TableAlreadyExistsException { message: String },
    BadRequestException { message: String },
    CommitFailedException { message: String },
    InternalServerError { message: String },
}

impl IcebergError {
    pub fn to_error_response(&self) -> ErrorResponse {
        let (message, error_type, code) = match self {
            IcebergError::NoSuchNamespaceException { message } => {
                (message.clone(), "NoSuchNamespaceException".to_string(), 404)
            }
            IcebergError::NamespaceAlreadyExistsException { message } => {
                (message.clone(), "NamespaceAlreadyExistsException".to_string(), 409)
            }
            IcebergError::NoSuchTableException { message } => {
                (message.clone(), "NoSuchTableException".to_string(), 404)
            }
            IcebergError::TableAlreadyExistsException { message } => {
                (message.clone(), "TableAlreadyExistsException".to_string(), 409)
            }
            IcebergError::BadRequestException { message } => {
                (message.clone(), "BadRequestException".to_string(), 400)
            }
            IcebergError::CommitFailedException { message } => {
                (message.clone(), "CommitFailedException".to_string(), 409)
            }
            IcebergError::InternalServerError { message } => {
                (message.clone(), "InternalServerError".to_string(), 500)
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

pub fn store_error_to_iceberg_namespace(err: StoreError) -> IcebergError {
    match err {
        StoreError::NotFound(msg) => IcebergError::NoSuchNamespaceException { message: msg },
        StoreError::AlreadyExists(msg) => {
            IcebergError::NamespaceAlreadyExistsException { message: msg }
        }
        StoreError::Conflict(msg) => {
            IcebergError::NamespaceAlreadyExistsException { message: msg }
        }
        StoreError::InvalidInput(msg) => IcebergError::BadRequestException { message: msg },
        StoreError::Internal(msg) => IcebergError::InternalServerError { message: msg },
    }
}

pub fn store_error_to_iceberg_table(err: StoreError) -> IcebergError {
    match err {
        StoreError::NotFound(msg) => IcebergError::NoSuchTableException { message: msg },
        StoreError::AlreadyExists(msg) => {
            IcebergError::TableAlreadyExistsException { message: msg }
        }
        StoreError::Conflict(msg) => IcebergError::CommitFailedException { message: msg },
        StoreError::InvalidInput(msg) => IcebergError::BadRequestException { message: msg },
        StoreError::Internal(msg) => IcebergError::InternalServerError { message: msg },
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
    fn test_store_error_to_iceberg_namespace_mapping() {
        assert!(matches!(
            store_error_to_iceberg_namespace(StoreError::NotFound("foo".into())),
            IcebergError::NoSuchNamespaceException { message } if message == "foo"
        ));
        assert!(matches!(
            store_error_to_iceberg_namespace(StoreError::AlreadyExists("foo".into())),
            IcebergError::NamespaceAlreadyExistsException { message } if message == "foo"
        ));
        assert!(matches!(
            store_error_to_iceberg_namespace(StoreError::Conflict("foo".into())),
            IcebergError::NamespaceAlreadyExistsException { message } if message == "foo"
        ));
        assert!(matches!(
            store_error_to_iceberg_namespace(StoreError::InvalidInput("bad".into())),
            IcebergError::BadRequestException { message } if message == "bad"
        ));
        assert!(matches!(
            store_error_to_iceberg_namespace(StoreError::Internal("oops".into())),
            IcebergError::InternalServerError { message } if message == "oops"
        ));
    }

    #[test]
    fn test_store_error_to_iceberg_table_mapping() {
        assert!(matches!(
            store_error_to_iceberg_table(StoreError::NotFound("bar".into())),
            IcebergError::NoSuchTableException { message } if message == "bar"
        ));
        assert!(matches!(
            store_error_to_iceberg_table(StoreError::AlreadyExists("bar".into())),
            IcebergError::TableAlreadyExistsException { message } if message == "bar"
        ));
        assert!(matches!(
            store_error_to_iceberg_table(StoreError::Conflict("conflict".into())),
            IcebergError::CommitFailedException { message } if message == "conflict"
        ));
        assert!(matches!(
            store_error_to_iceberg_table(StoreError::InvalidInput("bad".into())),
            IcebergError::BadRequestException { message } if message == "bad"
        ));
        assert!(matches!(
            store_error_to_iceberg_table(StoreError::Internal("oops".into())),
            IcebergError::InternalServerError { message } if message == "oops"
        ));
    }
}
