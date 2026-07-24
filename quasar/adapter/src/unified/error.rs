//! RFC-7807 Problem Details error responses for the Unified API
//! (DESIGN §5.5 / §7.6), extended with `code` and `request_id`.
//!
//! `code` is a closed set of six machine-readable codes mapped from
//! `CatalogError` variants per DESIGN §7.5. Error details are sanitized
//! per DESIGN §7.3: `Transient` and `Internal` contexts are logged, never
//! returned to the client.

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

/// Machine-readable error codes for the Unified API; the complete closed
/// set (DESIGN §7.5).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnifiedErrorCode {
    NotFound,
    AlreadyExists,
    Conflict,
    ValidationFailed,
    TransientError,
    InternalError,
}

impl UnifiedErrorCode {
    /// Machine-readable code string, e.g. `NOT_FOUND`.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::NotFound => "NOT_FOUND",
            Self::AlreadyExists => "ALREADY_EXISTS",
            Self::Conflict => "CONFLICT",
            Self::ValidationFailed => "VALIDATION_FAILED",
            Self::TransientError => "TRANSIENT_ERROR",
            Self::InternalError => "INTERNAL_ERROR",
        }
    }

    /// HTTP status code mapping (DESIGN §7.5).
    pub fn status_code(&self) -> StatusCode {
        match self {
            Self::NotFound => StatusCode::NOT_FOUND,
            Self::AlreadyExists | Self::Conflict => StatusCode::CONFLICT,
            Self::ValidationFailed => StatusCode::BAD_REQUEST,
            Self::TransientError => StatusCode::SERVICE_UNAVAILABLE,
            Self::InternalError => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    /// Human-readable title for the problem type.
    pub fn title(&self) -> &'static str {
        match self {
            Self::NotFound => "Not Found",
            Self::AlreadyExists => "Already Exists",
            Self::Conflict => "Conflict",
            Self::ValidationFailed => "Validation Failed",
            Self::TransientError => "Transient Error",
            Self::InternalError => "Internal Error",
        }
    }

    /// Slug used in the stable `type` URI: `https://quasar.io/errors/<slug>`.
    pub fn slug(&self) -> &'static str {
        match self {
            Self::NotFound => "not-found",
            Self::AlreadyExists => "already-exists",
            Self::Conflict => "conflict",
            Self::ValidationFailed => "validation-failed",
            Self::TransientError => "transient-error",
            Self::InternalError => "internal-error",
        }
    }
}

/// Unified API error rendered as `application/problem+json`.
pub struct UnifiedError {
    pub code: UnifiedErrorCode,
    pub detail: String,
    pub instance: String,
    pub request_id: String,
}

impl UnifiedError {
    pub fn new(
        code: UnifiedErrorCode,
        detail: impl Into<String>,
        instance: impl Into<String>,
        request_id: impl Into<String>,
    ) -> Self {
        Self {
            code,
            detail: detail.into(),
            instance: instance.into(),
            request_id: request_id.into(),
        }
    }
}

impl IntoResponse for UnifiedError {
    fn into_response(self) -> Response {
        let status = self.code.status_code();
        let problem = ProblemDetails {
            problem_type: format!("https://quasar.io/errors/{}", self.code.slug()),
            title: self.code.title().to_string(),
            status: status.as_u16(),
            detail: self.detail,
            instance: self.instance,
            code: self.code.as_str().to_string(),
            request_id: self.request_id,
        };

        let body = serde_json::to_string(&problem).unwrap_or_else(|_| {
            r#"{"type":"https://quasar.io/errors/internal-error","title":"Internal Error","status":500,"detail":"failed to serialize error response","instance":"","code":"INTERNAL_ERROR","request_id":""}"#.to_string()
        });

        (
            status,
            [(header::CONTENT_TYPE, "application/problem+json")],
            body,
        )
            .into_response()
    }
}

/// Map a `CatalogError` to a `UnifiedError` per DESIGN §7.5.
///
/// `Transient` and `Internal` contexts are sanitized (DESIGN §7.3): they
/// may carry connection strings or other internals, so they are logged
/// and replaced with a generic detail message.
pub fn map_catalog_error(err: CatalogError, instance: &str, request_id: &str) -> UnifiedError {
    match err {
        CatalogError::NotFound(msg) => {
            UnifiedError::new(UnifiedErrorCode::NotFound, msg, instance, request_id)
        }
        CatalogError::AlreadyExists(msg) => {
            UnifiedError::new(UnifiedErrorCode::AlreadyExists, msg, instance, request_id)
        }
        CatalogError::Conflict(msg) => {
            UnifiedError::new(UnifiedErrorCode::Conflict, msg, instance, request_id)
        }
        CatalogError::Validation(msg) => UnifiedError::new(
            UnifiedErrorCode::ValidationFailed,
            msg,
            instance,
            request_id,
        ),
        CatalogError::Transient(msg) => {
            tracing::warn!(error = %msg, "transient store error");
            UnifiedError::new(
                UnifiedErrorCode::TransientError,
                "service temporarily unavailable, please retry",
                instance,
                request_id,
            )
        }
        CatalogError::Internal(msg) => {
            tracing::error!(error = %msg, "internal store error");
            UnifiedError::new(
                UnifiedErrorCode::InternalError,
                "an internal error occurred",
                instance,
                request_id,
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_mapping_matches_design() {
        assert_eq!(
            UnifiedErrorCode::NotFound.status_code(),
            StatusCode::NOT_FOUND
        );
        assert_eq!(
            UnifiedErrorCode::AlreadyExists.status_code(),
            StatusCode::CONFLICT
        );
        assert_eq!(
            UnifiedErrorCode::Conflict.status_code(),
            StatusCode::CONFLICT
        );
        assert_eq!(
            UnifiedErrorCode::ValidationFailed.status_code(),
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            UnifiedErrorCode::TransientError.status_code(),
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(
            UnifiedErrorCode::InternalError.status_code(),
            StatusCode::INTERNAL_SERVER_ERROR
        );
    }

    #[test]
    fn internal_and_transient_details_are_sanitized() {
        let err = map_catalog_error(
            CatalogError::Internal("db password is hunter2".to_string()),
            "/test",
            "rid",
        );
        assert_eq!(err.code, UnifiedErrorCode::InternalError);
        assert!(!err.detail.contains("hunter2"));

        let err = map_catalog_error(
            CatalogError::Transient("pool at postgres://secret".to_string()),
            "/test",
            "rid",
        );
        assert_eq!(err.code, UnifiedErrorCode::TransientError);
        assert!(!err.detail.contains("secret"));
    }

    #[test]
    fn not_found_maps_to_404_code() {
        let err = map_catalog_error(
            CatalogError::NotFound("asset x".to_string()),
            "/test",
            "rid",
        );
        assert_eq!(err.code, UnifiedErrorCode::NotFound);
        assert_eq!(err.code.as_str(), "NOT_FOUND");
    }
}
