//! Shared object store utilities for protocol adapters.
//!
//! Provides format-agnostic helpers for reading/writing JSON to object storage.

use object_store::{path::Path, ObjectStore, PutPayload};

/// Convert an S3 URL (`s3://bucket/path`) to an object store relative `Path`.
///
/// Returns `None` if the location does not start with the expected bucket prefix.
pub fn s3_url_to_path(location: &str, bucket: &str) -> Option<Path> {
    let prefix = format!("s3://{}/", bucket);
    location.strip_prefix(&prefix).map(Path::from)
}

/// Serialize a JSON value to bytes and write it to object store.
///
/// # Errors
/// Returns `object_store::Error` on store write failure.
pub async fn write_json(
    store: &dyn ObjectStore,
    path: &Path,
    value: &serde_json::Value,
) -> Result<(), object_store::Error> {
    let bytes = serde_json::to_vec(value).map_err(|e| object_store::Error::Generic {
        store: "serde_json",
        source: Box::new(e),
    })?;
    store.put(path, PutPayload::from(bytes)).await?;
    Ok(())
}

/// Read bytes from object store and parse as JSON.
///
/// # Errors
/// Returns `object_store::Error` on read failure or JSON parse failure.
pub async fn read_json(
    store: &dyn ObjectStore,
    path: &Path,
) -> Result<serde_json::Value, object_store::Error> {
    let result = store.get(path).await?;
    let bytes = result.bytes().await?;
    serde_json::from_slice(&bytes).map_err(|e| object_store::Error::Generic {
        store: "serde_json",
        source: Box::new(e),
    })
}

/// Check whether an object exists using a HEAD request.
pub async fn object_exists(store: &dyn ObjectStore, path: &Path) -> bool {
    store.head(path).await.is_ok()
}
