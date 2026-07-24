//! Path dispatch for the `/iceberg/v1/{prefix}/namespaces/{*path}`
//! catch-all route.
//!
//! Hierarchical namespaces (D3) embed `/` in the namespace path, which a
//! single `{namespace}` path segment cannot capture. One catch-all route
//! per method therefore covers namespace CRUD plus the table/view
//! sub-resources; this module parses the reserved trailing segments and
//! delegates to the concrete handlers.
//!
//! Reserved trailing segments (`tables`, `views`, `properties`,
//! `register`, `metrics`) are matched from the end of the path. A
//! namespace whose own trailing segments collide with the reserved words
//! is shadowed by the sub-resource interpretation — an inherent ambiguity
//! of embedding the hierarchy in the URL path.

use axum::{
    extract::{Extension, Path, Query, State},
    http::HeaderMap,
    response::{IntoResponse, Response},
    Json,
};
use quasar_core::IcebergCatalogStore;
use serde::Deserialize;
use std::sync::Arc;

use super::dto::{
    CommitTableRequest, CommitViewRequest, CreateTableRequest, CreateViewRequest, ListTablesQuery,
    ListViewsQuery, RegisterTableRequest, UpdateNamespacePropertiesRequest,
};
use super::error::IcebergError;
use super::{namespace, table, view, CommitMetrics, IcebergConfig};

/// Superset of the query parameters used by the dispatched endpoints;
/// serde ignores fields that do not apply to the resolved target.
#[derive(Deserialize, Default)]
pub struct DispatchQuery {
    #[serde(default)]
    warehouse: Option<String>,
    #[serde(rename = "pageToken")]
    page_token: Option<String>,
    #[serde(rename = "pageSize")]
    page_size: Option<i32>,
    #[serde(rename = "purgeRequested")]
    purge_requested: Option<bool>,
}

/// The resource a `{*path}` wildcard resolves to. The `String` is always
/// the hierarchical namespace path (`a/b/c`).
enum NsPath {
    Namespace(String),
    NamespaceProperties(String),
    Register(String),
    Tables(String),
    TableItem(String, String),
    TableMetrics(String, String),
    Views(String),
    ViewItem(String, String),
}

/// Parse the wildcard path into a namespace path plus an optional
/// sub-resource, matching reserved trailing segments from the end.
fn parse_ns_path(path: &str) -> Result<NsPath, IcebergError> {
    let segs: Vec<&str> = path.split('/').collect();
    // The namespace path is every segment before the reserved suffix and
    // must not be empty.
    let ns = |n: usize| -> Result<String, IcebergError> {
        let prefix = segs[..n].join("/");
        if prefix.is_empty() {
            Err(IcebergError::BadRequestException {
                message: "namespace must not be empty".to_string(),
            })
        } else {
            Ok(prefix)
        }
    };

    match segs.as_slice() {
        [rest @ .., "tables", table, "metrics"] => {
            Ok(NsPath::TableMetrics(ns(rest.len())?, table.to_string()))
        }
        [rest @ .., "tables", table] => Ok(NsPath::TableItem(ns(rest.len())?, table.to_string())),
        [rest @ .., "tables"] => Ok(NsPath::Tables(ns(rest.len())?)),
        [rest @ .., "views", view] => Ok(NsPath::ViewItem(ns(rest.len())?, view.to_string())),
        [rest @ .., "views"] => Ok(NsPath::Views(ns(rest.len())?)),
        [rest @ .., "properties"] => Ok(NsPath::NamespaceProperties(ns(rest.len())?)),
        [rest @ .., "register"] => Ok(NsPath::Register(ns(rest.len())?)),
        _ => Ok(NsPath::Namespace(path.to_string())),
    }
}

/// Deserialize the JSON body into the concrete request type of the
/// resolved endpoint.
fn from_body<T: serde::de::DeserializeOwned>(body: serde_json::Value) -> Result<T, IcebergError> {
    serde_json::from_value(body).map_err(|e| IcebergError::BadRequestException {
        message: format!("invalid request body: {}", e),
    })
}

fn unsupported(method: &str, path: &str) -> IcebergError {
    IcebergError::BadRequestException {
        message: format!("{} is not supported on path '/namespaces/{}'", method, path),
    }
}

/// GET /iceberg/v1/{prefix}/namespaces/{*path}
pub async fn get(
    State(store): State<Arc<dyn IcebergCatalogStore>>,
    Path((prefix, path)): Path<(String, String)>,
    Query(query): Query<DispatchQuery>,
    Extension(config): Extension<IcebergConfig>,
) -> Result<Response, IcebergError> {
    let warehouse = query.warehouse.as_deref();
    match parse_ns_path(&path)? {
        NsPath::Namespace(ns) => Ok(namespace::get_namespace(
            &store, &config, &prefix, &ns, warehouse,
        )
        .await?
        .into_response()),
        NsPath::Tables(ns) => Ok(table::list_tables(
            &store,
            &config,
            &prefix,
            &ns,
            ListTablesQuery {
                page_token: query.page_token,
                page_size: query.page_size,
                warehouse: query.warehouse,
            },
        )
        .await?
        .into_response()),
        NsPath::Views(ns) => Ok(view::list_views(
            &store,
            &config,
            &prefix,
            &ns,
            ListViewsQuery {
                page_token: query.page_token,
                page_size: query.page_size,
                warehouse: query.warehouse,
            },
        )
        .await?
        .into_response()),
        NsPath::TableItem(ns, table) => Ok(table::load_table(
            &store, &config, &prefix, &ns, &table, warehouse,
        )
        .await?
        .into_response()),
        NsPath::ViewItem(ns, view_name) => Ok(view::load_view(
            &store, &config, &prefix, &ns, &view_name, warehouse,
        )
        .await?
        .into_response()),
        _ => Err(unsupported("GET", &path)),
    }
}

/// HEAD /iceberg/v1/{prefix}/namespaces/{*path}
pub async fn head(
    State(store): State<Arc<dyn IcebergCatalogStore>>,
    Path((prefix, path)): Path<(String, String)>,
    Query(query): Query<DispatchQuery>,
    Extension(config): Extension<IcebergConfig>,
) -> Result<Response, IcebergError> {
    let warehouse = query.warehouse.as_deref();
    let status = match parse_ns_path(&path)? {
        NsPath::Namespace(ns) => {
            namespace::namespace_exists(&store, &config, &prefix, &ns, warehouse).await?
        }
        NsPath::TableItem(ns, table) => {
            table::table_exists(&store, &config, &prefix, &ns, &table, warehouse).await?
        }
        NsPath::ViewItem(ns, view) => {
            view::view_exists(&store, &config, &prefix, &ns, &view, warehouse).await?
        }
        _ => return Err(unsupported("HEAD", &path)),
    };
    Ok(status.into_response())
}

/// DELETE /iceberg/v1/{prefix}/namespaces/{*path}
pub async fn delete(
    State(store): State<Arc<dyn IcebergCatalogStore>>,
    Path((prefix, path)): Path<(String, String)>,
    Query(query): Query<DispatchQuery>,
    Extension(config): Extension<IcebergConfig>,
) -> Result<Response, IcebergError> {
    let warehouse = query.warehouse.as_deref();
    let status = match parse_ns_path(&path)? {
        NsPath::Namespace(ns) => {
            namespace::drop_namespace(&store, &config, &prefix, &ns, warehouse).await?
        }
        NsPath::TableItem(ns, table) => {
            table::drop_table(
                &store,
                &config,
                &prefix,
                &ns,
                &table,
                warehouse,
                query.purge_requested,
            )
            .await?
        }
        NsPath::ViewItem(ns, view) => {
            view::drop_view(&store, &config, &prefix, &ns, &view, warehouse).await?
        }
        _ => return Err(unsupported("DELETE", &path)),
    };
    Ok(status.into_response())
}

/// POST /iceberg/v1/{prefix}/namespaces/{*path}
pub async fn post(
    State(store): State<Arc<dyn IcebergCatalogStore>>,
    Path((prefix, path)): Path<(String, String)>,
    Query(query): Query<DispatchQuery>,
    Extension(config): Extension<IcebergConfig>,
    metrics: Option<Extension<CommitMetrics>>,
    headers: HeaderMap,
    Json(body): Json<serde_json::Value>,
) -> Result<Response, IcebergError> {
    let warehouse = query.warehouse.as_deref();
    // Commit outcome metrics; no-op when the server did not install a sink.
    let commit_metrics = metrics.map(|e| e.0).unwrap_or_default();
    match parse_ns_path(&path)? {
        NsPath::NamespaceProperties(ns) => {
            let req: UpdateNamespacePropertiesRequest = from_body(body)?;
            Ok(namespace::update_namespace_properties(
                &store, &config, &prefix, &ns, warehouse, req,
            )
            .await?
            .into_response())
        }
        NsPath::Register(ns) => {
            let req: RegisterTableRequest = from_body(body)?;
            Ok(
                table::register_table(&store, &config, &prefix, &ns, warehouse, req)
                    .await?
                    .into_response(),
            )
        }
        NsPath::Tables(ns) => {
            let req: CreateTableRequest = from_body(body)?;
            Ok(
                table::create_table(&store, &config, &prefix, &ns, warehouse, req)
                    .await?
                    .into_response(),
            )
        }
        NsPath::TableItem(ns, table_name) => {
            let req: CommitTableRequest = from_body(body)?;
            Ok(table::commit_table(
                &store,
                &config,
                commit_metrics,
                &prefix,
                &ns,
                &table_name,
                warehouse,
                req,
            )
            .await?
            .into_response())
        }
        NsPath::TableMetrics(ns, table_name) => {
            let user_agent = headers.get("user-agent").and_then(|v| v.to_str().ok());
            let status = table::report_metrics(
                &store,
                &config,
                &prefix,
                &ns,
                &table_name,
                warehouse,
                user_agent,
                body,
            )
            .await?;
            Ok(status.into_response())
        }
        NsPath::Views(ns) => {
            let req: CreateViewRequest = from_body(body)?;
            Ok(
                view::create_view(&store, &config, &prefix, &ns, warehouse, req)
                    .await?
                    .into_response(),
            )
        }
        NsPath::ViewItem(ns, view_name) => {
            let req: CommitViewRequest = from_body(body)?;
            Ok(view::replace_view(
                &store,
                &config,
                commit_metrics,
                &prefix,
                &ns,
                &view_name,
                warehouse,
                req,
            )
            .await?
            .into_response())
        }
        NsPath::Namespace(_) => Err(unsupported("POST", &path)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_single_segment_namespace() {
        assert!(matches!(
            parse_ns_path("prod"),
            Ok(NsPath::Namespace(ns)) if ns == "prod"
        ));
    }

    #[test]
    fn parses_hierarchical_namespace() {
        assert!(matches!(
            parse_ns_path("a/b/c"),
            Ok(NsPath::Namespace(ns)) if ns == "a/b/c"
        ));
    }

    #[test]
    fn parses_table_subresources() {
        assert!(matches!(
            parse_ns_path("a/b/tables"),
            Ok(NsPath::Tables(ns)) if ns == "a/b"
        ));
        assert!(matches!(
            parse_ns_path("a/b/tables/t1"),
            Ok(NsPath::TableItem(ns, t)) if ns == "a/b" && t == "t1"
        ));
        assert!(matches!(
            parse_ns_path("a/b/tables/t1/metrics"),
            Ok(NsPath::TableMetrics(ns, t)) if ns == "a/b" && t == "t1"
        ));
    }

    #[test]
    fn parses_view_subresources() {
        assert!(matches!(
            parse_ns_path("a/views"),
            Ok(NsPath::Views(ns)) if ns == "a"
        ));
        assert!(matches!(
            parse_ns_path("a/b/views/v1"),
            Ok(NsPath::ViewItem(ns, v)) if ns == "a/b" && v == "v1"
        ));
    }

    #[test]
    fn parses_namespace_properties_and_register() {
        assert!(matches!(
            parse_ns_path("a/b/properties"),
            Ok(NsPath::NamespaceProperties(ns)) if ns == "a/b"
        ));
        assert!(matches!(
            parse_ns_path("a/b/register"),
            Ok(NsPath::Register(ns)) if ns == "a/b"
        ));
    }

    #[test]
    fn rejects_empty_namespace_for_subresources() {
        assert!(parse_ns_path("tables").is_err());
        assert!(parse_ns_path("properties").is_err());
    }
}
