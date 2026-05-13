use quasar_core::{AssetFormat, AssetVersion, CatalogStore, TabularAssetVersion};

use super::dto::CurrentVersionResponse;
use super::error::{UnifiedError, UnifiedErrorCode};

use super::UnifiedConfig;
use crate::DEFAULT_DOMAIN;

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
            get_lance_current_version(store, namespace, name, instance, request_id).await
        }
        AssetFormat::Iceberg => get_iceberg_current_version(config, metadata_location).await,
    }
}

// ── Lance ───────────────────────────────────────────────────────

async fn get_lance_current_version(
    store: &dyn CatalogStore,
    namespace: &str,
    name: &str,
    instance: &str,
    request_id: &str,
) -> Result<Option<CurrentVersionResponse>, UnifiedError> {
    // Resolve asset id (lance format) and then ask the version store for
    // the latest tabular version; V3 splits the lookup from the version
    // operation.
    let (asset, _) = store
        .get_tabular_asset(DEFAULT_DOMAIN, namespace, "lance", name)
        .await
        .map_err(|e| {
            UnifiedError::new(
                UnifiedErrorCode::InternalError,
                format!("failed to resolve lance asset: {}", e),
                instance,
                request_id,
            )
        })?;

    let version = store
        .get_latest_tabular_version(asset.id)
        .await
        .map_err(|e| {
            UnifiedError::new(
                UnifiedErrorCode::InternalError,
                format!("failed to load current version: {}", e),
                instance,
                request_id,
            )
        })?;

    version
        .map(|pair| lance_version_to_response(pair, instance, request_id))
        .transpose()
}

fn lance_version_to_response(
    pair: (AssetVersion, TabularAssetVersion),
    instance: &str,
    request_id: &str,
) -> Result<CurrentVersionResponse, UnifiedError> {
    let (version, tabular_version) = pair;
    let version_id = version.version_order.ok_or_else(|| {
        tracing::error!(version_id = %version.id, version_key = %version.version_key,
            "lance current_version returned version without version_order");
        UnifiedError::new(
            UnifiedErrorCode::InternalError,
            "An internal error occurred",
            instance,
            request_id,
        )
    })?;
    Ok(CurrentVersionResponse::Lance {
        version_id,
        metadata_location: tabular_version.metadata_location,
        // V3 model stores previous_version_id as Uuid on AssetVersion (not as
        // i64 on tabular_version). The Lance protocol response expects an i64
        // version number. Until the storage layer exposes a Uuid ->
        // version_order resolver, expose None and rely on the explicit version
        // chain that V3 will surface from queries.
        previous_version_id: None,
        timestamp: version.created_at.to_rfc3339(),
    })
}

// ── Iceberg ─────────────────────────────────────────────────────

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
