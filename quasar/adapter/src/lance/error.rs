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
        let status = StatusCode::from_u16(self.code)
            .unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
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
        StoreError::Conflict(msg) => LanceError::NamespaceNotEmpty {
            name: msg,
            instance: instance.to_string(),
        },
        StoreError::InvalidInput(msg) => LanceError::InvalidInput {
            detail: msg,
            instance: instance.to_string(),
        },
        StoreError::Internal(msg) => LanceError::InternalError {
            detail: msg,
            instance: instance.to_string(),
        },
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
        StoreError::Conflict(msg) => LanceError::TableNotEmpty {
            name: msg,
            instance: instance.to_string(),
        },
        StoreError::InvalidInput(msg) => LanceError::InvalidInput {
            detail: msg,
            instance: instance.to_string(),
        },
        StoreError::Internal(msg) => LanceError::InternalError {
            detail: msg,
            instance: instance.to_string(),
        },
    }
}

pub fn store_error_to_lance_version(err: StoreError, instance: &str) -> LanceError {
    match err {
        StoreError::NotFound(msg) => LanceError::TableNotFound {
            name: msg,
            instance: instance.to_string(),
        },
        StoreError::AlreadyExists(msg) => {
            if msg.starts_with("version ") {
                let version_str = msg
                    .trim_start_matches("version ")
                    .split(' ')
                    .next()
                    .unwrap_or("0");
                let version = version_str.parse::<i64>().unwrap_or(0);
                LanceError::TableVersionAlreadyExists {
                    version,
                    instance: instance.to_string(),
                }
            } else {
                LanceError::TableAlreadyExists {
                    name: msg,
                    instance: instance.to_string(),
                }
            }
        }
        StoreError::Conflict(msg) => LanceError::TableNotEmpty {
            name: msg,
            instance: instance.to_string(),
        },
        StoreError::InvalidInput(msg) => LanceError::InvalidInput {
            detail: msg,
            instance: instance.to_string(),
        },
        StoreError::Internal(msg) => LanceError::InternalError {
            detail: msg,
            instance: instance.to_string(),
        },
    }
}
