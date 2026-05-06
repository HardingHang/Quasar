use quasar_core::{AssetFormat, AssetVersionWithTabular, CatalogStore};

use super::dto::CurrentVersionResponse;
use super::error::{UnifiedError, UnifiedErrorCode};

use super::UnifiedConfig;

/// Get the current version for an asset, format-specific.
#[allow(clippy::too_many_arguments)]
pub async fn get_current_version(
    store: &dyn CatalogStore,
    config: &UnifiedConfig,
    namespace: &str,
    name: &str,
    format: AssetFormat,
    metadata_location: Option<&str>,
    instance: &str,
    request_id: &str,
) -> Result<Option<CurrentVersionResponse>, UnifiedError> {
    match format {
        AssetFormat::Lance => {
            get_lance_current_version(store, namespace, name, format, instance, request_id).await
        }
        #[cfg(feature = "iceberg")]
        AssetFormat::Iceberg => get_iceberg_current_version(config, metadata_location).await,
        #[cfg(not(feature = "iceberg"))]
        AssetFormat::Iceberg => Ok(None),
    }
}

// ── Lance ───────────────────────────────────────────────────────

async fn get_lance_current_version(
    store: &dyn CatalogStore,
    namespace: &str,
    name: &str,
    format: AssetFormat,
    instance: &str,
    request_id: &str,
) -> Result<Option<CurrentVersionResponse>, UnifiedError> {
    let version = store
        .load_current_version(namespace, format, name)
        .await
        .map_err(|e| {
            UnifiedError::new(
                UnifiedErrorCode::InternalError,
                format!("failed to load current version: {}", e),
                instance,
                request_id,
            )
        })?;

    Ok(version.map(lance_version_to_response))
}

fn lance_version_to_response(v: AssetVersionWithTabular) -> CurrentVersionResponse {
    CurrentVersionResponse::Lance {
        version_id: v
            .version
            .version_order
            .unwrap_or_else(|| v.version.version_key.parse().unwrap_or(0)),
        metadata_location: v.tabular_version.metadata_location,
        previous_version_id: v.tabular_version.previous_version_order,
        timestamp: v.version.created_at.to_rfc3339(),
    }
}

// ── Iceberg ─────────────────────────────────────────────────────

#[cfg(feature = "iceberg")]
async fn get_iceberg_current_version(
    config: &UnifiedConfig,
    metadata_location: Option<&str>,
) -> Result<Option<CurrentVersionResponse>, UnifiedError> {
    let Some(object_store) = config.object_store.as_ref() else {
        return Ok(None);
    };
    let Some(bucket) = config.s3_bucket.as_deref() else {
        return Ok(None);
    };
    let Some(loc) = metadata_location else {
        return Ok(None);
    };

    let path = match crate::object_store_util::s3_url_to_path(loc, bucket) {
        Some(p) => p,
        None => {
            tracing::warn!(
                "failed to parse metadata_location '{}' with bucket '{}'",
                loc,
                bucket
            );
            return Ok(None);
        }
    };

    let metadata = match crate::object_store_util::read_json(object_store.as_ref(), &path).await {
        Ok(v) => v,
        Err(e) => {
            tracing::warn!("failed to read metadata.json at {}: {}", loc, e);
            return Ok(None);
        }
    };

    let sequence_number = metadata
        .get("last-sequence-number")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);

    let current_snapshot_id = metadata.get("current-snapshot-id").and_then(|v| v.as_i64());

    let timestamp_ms = if let Some(snapshot_id) = current_snapshot_id {
        metadata
            .get("snapshots")
            .and_then(|s| s.as_array())
            .and_then(|snapshots| {
                snapshots
                    .iter()
                    .find(|s| s.get("snapshot-id").and_then(|id| id.as_i64()) == Some(snapshot_id))
            })
            .and_then(|s| s.get("timestamp-ms").and_then(|t| t.as_i64()))
    } else {
        None
    };

    Ok(Some(CurrentVersionResponse::Iceberg {
        sequence_number,
        snapshot_id: current_snapshot_id,
        timestamp_ms,
    }))
}
