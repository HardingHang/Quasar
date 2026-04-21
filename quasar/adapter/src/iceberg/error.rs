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
