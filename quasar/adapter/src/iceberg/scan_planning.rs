use axum::{
    extract::{Extension, Json, Path, Query, State},
    http::StatusCode,
    response::IntoResponse,
};
use base64::Engine;
use quasar_core::{CatalogStore, StoreError};
use std::sync::Arc;

use super::dto::{
    FetchPlanResponse, FetchTasksRequest, FileScanTask, SubmitPlanRequest, SubmitPlanResponse,
    WarehouseQuery,
};
use super::error::{store_error_to_iceberg_table, IcebergError};
use super::{validate_warehouse, IcebergConfig};
use crate::object_store_util::{read_json, s3_url_to_path};

/// Read metadata JSON from object store if configured, otherwise fallback.
async fn read_metadata_from_store(
    location: &str,
    fallback: Option<serde_json::Value>,
    config: &IcebergConfig,
) -> Result<serde_json::Value, IcebergError> {
    if let (Some(ref store), Some(ref bucket)) = (&config.object_store, &config.s3_bucket) {
        let path =
            s3_url_to_path(location, bucket).ok_or_else(|| IcebergError::InternalServerError {
                message: format!("invalid metadata location: {}", location),
            })?;
        read_json(&**store, &path)
            .await
            .map_err(|e| IcebergError::InternalServerError {
                message: format!("failed to read metadata from object store: {}", e),
            })
    } else if let Some(fb) = fallback {
        Ok(fb)
    } else {
        Err(IcebergError::InternalServerError {
            message: "no metadata available".to_string(),
        })
    }
}

/// Token payload encoded into the plan-task string.
#[derive(serde::Serialize, serde::Deserialize)]
struct PlanTaskToken {
    #[serde(rename = "plan-id")]
    plan_id: String,
    #[serde(rename = "table-id")]
    table_id: super::dto::TableIdentifier,
    #[serde(rename = "snapshot-id")]
    snapshot_id: i64,
    tasks: Vec<FileScanTask>,
}

fn encode_plan_task(token: &PlanTaskToken) -> Result<String, String> {
    let json = serde_json::to_vec(token).map_err(|e| e.to_string())?;
    Ok(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&json))
}

fn decode_plan_task(token: &str) -> Result<PlanTaskToken, String> {
    let json = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(token)
        .map_err(|e| format!("base64 decode: {e}"))?;
    serde_json::from_slice(&json).map_err(|e| e.to_string())
}

/// POST /iceberg/v1/{prefix}/namespaces/{ns}/tables/{table}/plan
pub async fn submit_plan(
    State(store): State<Arc<dyn CatalogStore>>,
    Extension(config): Extension<IcebergConfig>,
    Path((prefix, ns, table)): Path<(String, String, String)>,
    Query(query): Query<WarehouseQuery>,
    Json(req): Json<SubmitPlanRequest>,
) -> Result<impl IntoResponse, IcebergError> {
    validate_warehouse(query.warehouse.as_deref(), &config)?;

    let (asset, tabular) = store
        .get_tabular_asset(&prefix, &ns, "iceberg", &table)
        .await
        .map_err(|e| match e {
            StoreError::NotFound(msg) => IcebergError::NoSuchTableException { message: msg },
            other => store_error_to_iceberg_table(other),
        })?;

    let metadata_location =
        tabular
            .metadata_location
            .ok_or_else(|| IcebergError::InternalServerError {
                message: format!("Table '{}.{}' has no metadata location", ns, table),
            })?;

    let metadata_json =
        read_metadata_from_store(&metadata_location, tabular.schema_snapshot.clone(), &config)
            .await?;

    // Determine snapshot-id: use request value or current-snapshot-id from metadata
    let snapshot_id = req.snapshot_id.unwrap_or_else(|| {
        metadata_json
            .get("current-snapshot-id")
            .and_then(|v| v.as_i64())
            .unwrap_or(0)
    });

    if snapshot_id == 0 {
        return Err(IcebergError::NoSuchSnapshotException {
            message: "Table has no snapshots".to_string(),
        });
    }

    // V4.2 simplified: return empty task list.
    // Full implementation would call iceberg crate scan API.
    let plan_id = uuid::Uuid::new_v4().to_string();
    let token = PlanTaskToken {
        plan_id: plan_id.clone(),
        table_id: super::dto::TableIdentifier {
            namespace: vec![ns.clone()],
            name: table.clone(),
        },
        snapshot_id,
        tasks: vec![],
    };

    let plan_task = encode_plan_task(&token).map_err(|e| IcebergError::InternalServerError {
        message: format!("failed to encode plan-task token: {e}"),
    })?;

    // Record scan metrics if available
    let _ = asset;

    Ok((
        StatusCode::OK,
        Json(SubmitPlanResponse { plan_id, plan_task }),
    ))
}

/// GET /iceberg/v1/{prefix}/namespaces/{ns}/tables/{table}/plan/{plan_id}
pub async fn fetch_plan(
    Path((_prefix, _ns, _table, _plan_id)): Path<(String, String, String, String)>,
) -> Result<impl IntoResponse, IcebergError> {
    // V4.2 simplified: plans are completed synchronously in submit_plan.
    // Any valid plan_id is treated as completed.
    Ok((
        StatusCode::OK,
        Json(FetchPlanResponse {
            status: "completed".to_string(),
            plan_task: None,
        }),
    ))
}

/// DELETE /iceberg/v1/{prefix}/namespaces/{ns}/tables/{table}/plan/{plan_id}
pub async fn cancel_plan(
    Path((_prefix, _ns, _table, _plan_id)): Path<(String, String, String, String)>,
) -> Result<impl IntoResponse, IcebergError> {
    // V4.2 simplified: plans complete synchronously; cancel is a no-op.
    Ok(StatusCode::NO_CONTENT)
}

/// POST /iceberg/v1/{prefix}/namespaces/{ns}/tables/{table}/tasks
pub async fn fetch_tasks(
    Path((_prefix, _ns, _table)): Path<(String, String, String)>,
    Json(req): Json<FetchTasksRequest>,
) -> Result<impl IntoResponse, IcebergError> {
    let token =
        decode_plan_task(&req.plan_task).map_err(|e| IcebergError::BadRequestException {
            message: format!("invalid plan-task token: {e}"),
        })?;

    Ok((StatusCode::OK, Json(token.tasks)))
}

// ── Alias handlers (Java ResourcePaths paths, without namespace segment) ──

/// POST /iceberg/v1/{prefix}/tables/{table}/plan
pub async fn submit_plan_alias(
    State(store): State<Arc<dyn CatalogStore>>,
    Extension(config): Extension<IcebergConfig>,
    Path((prefix, table)): Path<(String, String)>,
    Query(query): Query<WarehouseQuery>,
    Json(req): Json<SubmitPlanRequest>,
) -> Result<impl IntoResponse, IcebergError> {
    // Try to infer namespace from table path parameter
    let (ns, table_name) = parse_table_ident(&table);
    submit_plan(
        State(store),
        Extension(config),
        Path((prefix, ns, table_name)),
        Query(query),
        Json(req),
    )
    .await
}

/// GET /iceberg/v1/{prefix}/tables/{table}/plan/{plan_id}
pub async fn fetch_plan_alias(
    Path((prefix, table, plan_id)): Path<(String, String, String)>,
) -> Result<impl IntoResponse, IcebergError> {
    let (ns, table_name) = parse_table_ident(&table);
    fetch_plan(Path((prefix, ns, table_name, plan_id))).await
}

/// DELETE /iceberg/v1/{prefix}/tables/{table}/plan/{plan_id}
pub async fn cancel_plan_alias(
    Path((prefix, table, plan_id)): Path<(String, String, String)>,
) -> Result<impl IntoResponse, IcebergError> {
    let (ns, table_name) = parse_table_ident(&table);
    cancel_plan(Path((prefix, ns, table_name, plan_id))).await
}

/// POST /iceberg/v1/{prefix}/tables/{table}/tasks
pub async fn fetch_tasks_alias(
    Path((prefix, table)): Path<(String, String)>,
    Json(req): Json<FetchTasksRequest>,
) -> Result<impl IntoResponse, IcebergError> {
    let (ns, table_name) = parse_table_ident(&table);
    fetch_tasks(Path((prefix, ns, table_name)), Json(req)).await
}

/// Parse a table identifier that may contain namespace segments separated by
/// the unit separator (\x1F) used by Iceberg Java clients.
///
/// Returns (namespace, table_name).
fn parse_table_ident(table: &str) -> (String, String) {
    // Iceberg Java TableIdent.toUrlString() uses \x1F as separator.
    // Format: {ns1}\x1F{ns2}\x1F...\x1F{name}
    let parts: Vec<&str> = table.split('\u{001F}').collect();
    if parts.len() >= 2 {
        let name = parts.last().map(|s| s.to_string()).unwrap_or_default();
        let ns = parts[..parts.len() - 1].join(".");
        (ns, name)
    } else {
        // No namespace separator: use empty namespace
        ("".to_string(), table.to_string())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use super::super::dto::DataFile;
    use super::*;

    #[test]
    fn test_plan_task_token_roundtrip() {
        let token = PlanTaskToken {
            plan_id: "plan-123".to_string(),
            table_id: super::super::dto::TableIdentifier {
                namespace: vec!["prod".to_string()],
                name: "users".to_string(),
            },
            snapshot_id: 42,
            tasks: vec![FileScanTask {
                data_file: DataFile {
                    file_path: "s3://bucket/data/file.parquet".to_string(),
                    file_format: "PARQUET".to_string(),
                    partition: serde_json::json!({}),
                    record_count: 1000,
                    file_size_in_bytes: 50000,
                    column_masks: None,
                },
                delete_files: vec![],
                start: 0,
                length: 50000,
            }],
        };

        let encoded = encode_plan_task(&token).unwrap();
        let decoded = decode_plan_task(&encoded).unwrap();
        assert_eq!(decoded.plan_id, token.plan_id);
        assert_eq!(decoded.snapshot_id, token.snapshot_id);
        assert_eq!(decoded.tasks.len(), 1);
    }

    #[test]
    fn test_parse_table_ident_with_namespace() {
        let (ns, name) = parse_table_ident("db\u{001F}events");
        assert_eq!(ns, "db");
        assert_eq!(name, "events");
    }

    #[test]
    fn test_parse_table_ident_multi_level() {
        let (ns, name) = parse_table_ident("a\u{001F}b\u{001F}c\u{001F}events");
        assert_eq!(ns, "a.b.c");
        assert_eq!(name, "events");
    }

    #[test]
    fn test_parse_table_ident_no_separator() {
        let (ns, name) = parse_table_ident("events");
        assert_eq!(ns, "");
        assert_eq!(name, "events");
    }
}
