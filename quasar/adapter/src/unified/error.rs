use axum::{
    http::{header, StatusCode},
    response::{IntoResponse, Response},
};
use quasar_core::StoreError;
use serde::Serialize;

/// RFC 7807 Problem Details response body for Unified API.
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

/// Machine-readable error codes for the Unified API.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnifiedErrorCode {
    NamespaceNotFound,
    NamespaceAlreadyExists,
    NamespaceNotEmpty,
    AssetNotFound,
    AssetAlreadyExists,
    InvalidInput,
    InvalidFormat,
    InvalidPageToken,
    PageSizeTooLarge,
    MethodNotAllowed,
    Conflict,
    ServiceUnavailable,
    InternalError,
}

impl UnifiedErrorCode {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::NamespaceNotFound => "NamespaceNotFound",
            Self::NamespaceAlreadyExists => "NamespaceAlreadyExists",
            Self::NamespaceNotEmpty => "NamespaceNotEmpty",
            Self::AssetNotFound => "AssetNotFound",
            Self::AssetAlreadyExists => "AssetAlreadyExists",
            Self::InvalidInput => "InvalidInput",
            Self::InvalidFormat => "InvalidFormat",
            Self::InvalidPageToken => "InvalidPageToken",
            Self::PageSizeTooLarge => "PageSizeTooLarge",
            Self::MethodNotAllowed => "MethodNotAllowed",
            Self::Conflict => "Conflict",
            Self::ServiceUnavailable => "ServiceUnavailable",
            Self::InternalError => "InternalError",
        }
    }

    pub fn status_code(&self) -> StatusCode {
        match self {
            Self::NamespaceNotFound | Self::AssetNotFound => StatusCode::NOT_FOUND,
            Self::NamespaceAlreadyExists
            | Self::AssetAlreadyExists
            | Self::NamespaceNotEmpty
            | Self::Conflict => StatusCode::CONFLICT,
            Self::InvalidInput
            | Self::InvalidFormat
            | Self::InvalidPageToken
            | Self::PageSizeTooLarge => StatusCode::BAD_REQUEST,
            Self::MethodNotAllowed => StatusCode::METHOD_NOT_ALLOWED,
            Self::ServiceUnavailable => StatusCode::SERVICE_UNAVAILABLE,
            Self::InternalError => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    pub fn problem_slug(&self) -> &'static str {
        match self {
            Self::NamespaceNotFound => "namespace-not-found",
            Self::NamespaceAlreadyExists => "namespace-already-exists",
            Self::NamespaceNotEmpty => "namespace-not-empty",
            Self::AssetNotFound => "asset-not-found",
            Self::AssetAlreadyExists => "asset-already-exists",
            Self::InvalidInput => "invalid-input",
            Self::InvalidFormat => "invalid-format",
            Self::InvalidPageToken => "invalid-page-token",
            Self::PageSizeTooLarge => "page-size-too-large",
            Self::MethodNotAllowed => "method-not-allowed",
            Self::Conflict => "conflict",
            Self::ServiceUnavailable => "service-unavailable",
            Self::InternalError => "internal-error",
        }
    }
}

/// Unified API-specific error type.
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
            problem_type: format!("https://quasar.dev/problems/{}", self.code.problem_slug()),
            title: self.code.as_str().to_string(),
            status: status.as_u16(),
            detail: self.detail,
            instance: self.instance,
            code: self.code.as_str().to_string(),
            request_id: self.request_id,
        };

        let body = serde_json::to_string(&problem).unwrap_or_else(|_| {
            r#"{"type":"https://quasar.dev/problems/internal-error","title":"InternalError","status":500,"detail":"failed to serialize error response","instance":"","code":"InternalError","request_id":""}"#.to_string()
        });

        (
            status,
            [(header::CONTENT_TYPE, "application/problem+json")],
            body,
        )
            .into_response()
    }
}

/// Map StoreError to UnifiedError for namespace operations.
pub fn map_namespace_error(err: StoreError, instance: &str, request_id: &str) -> UnifiedError {
    match err {
        StoreError::NotFound(msg) => UnifiedError::new(
            UnifiedErrorCode::NamespaceNotFound,
            msg,
            instance,
            request_id,
        ),
        StoreError::AlreadyExists(msg) => UnifiedError::new(
            UnifiedErrorCode::NamespaceAlreadyExists,
            msg,
            instance,
            request_id,
        ),
        StoreError::NamespaceNotEmpty { namespace } => UnifiedError::new(
            UnifiedErrorCode::NamespaceNotEmpty,
            format!("namespace '{}' is not empty", namespace),
            instance,
            request_id,
        ),
        StoreError::DomainNotEmpty { domain } => UnifiedError::new(
            UnifiedErrorCode::NamespaceNotEmpty,
            format!("domain '{}' is not empty", domain),
            instance,
            request_id,
        ),
        StoreError::Conflict { msg } => {
            UnifiedError::new(UnifiedErrorCode::Conflict, msg, instance, request_id)
        }
        StoreError::InvalidInput(msg) => {
            UnifiedError::new(UnifiedErrorCode::InvalidInput, msg, instance, request_id)
        }
        StoreError::DatabaseUnavailable { .. } => UnifiedError::new(
            UnifiedErrorCode::ServiceUnavailable,
            "service temporarily unavailable",
            instance,
            request_id,
        ),
        StoreError::Timeout { operation } => UnifiedError::new(
            UnifiedErrorCode::ServiceUnavailable,
            format!("operation '{}' timed out", operation),
            instance,
            request_id,
        ),
        StoreError::Internal { msg, .. } => {
            UnifiedError::new(UnifiedErrorCode::InternalError, msg, instance, request_id)
        }
    }
}

/// Map StoreError to UnifiedError for asset operations.
pub fn map_asset_error(err: StoreError, instance: &str, request_id: &str) -> UnifiedError {
    match err {
        StoreError::NotFound(msg) => {
            UnifiedError::new(UnifiedErrorCode::AssetNotFound, msg, instance, request_id)
        }
        StoreError::AlreadyExists(msg) => UnifiedError::new(
            UnifiedErrorCode::AssetAlreadyExists,
            msg,
            instance,
            request_id,
        ),
        StoreError::NamespaceNotEmpty { namespace } => UnifiedError::new(
            UnifiedErrorCode::NamespaceNotEmpty,
            format!("namespace '{}' is not empty", namespace),
            instance,
            request_id,
        ),
        StoreError::DomainNotEmpty { domain } => UnifiedError::new(
            UnifiedErrorCode::NamespaceNotEmpty,
            format!("domain '{}' is not empty", domain),
            instance,
            request_id,
        ),
        StoreError::Conflict { msg } => {
            UnifiedError::new(UnifiedErrorCode::Conflict, msg, instance, request_id)
        }
        StoreError::InvalidInput(msg) => {
            UnifiedError::new(UnifiedErrorCode::InvalidInput, msg, instance, request_id)
        }
        StoreError::DatabaseUnavailable { .. } => UnifiedError::new(
            UnifiedErrorCode::ServiceUnavailable,
            "service temporarily unavailable",
            instance,
            request_id,
        ),
        StoreError::Timeout { operation } => UnifiedError::new(
            UnifiedErrorCode::ServiceUnavailable,
            format!("operation '{}' timed out", operation),
            instance,
            request_id,
        ),
        StoreError::Internal { msg, .. } => {
            UnifiedError::new(UnifiedErrorCode::InternalError, msg, instance, request_id)
        }
    }
}
