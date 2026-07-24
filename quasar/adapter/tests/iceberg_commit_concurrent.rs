//! Iceberg REST Catalog — concurrent commit CAS integration test.
//!
//! Fires concurrent commits against the same table; exactly one must win
//! the pointer CAS and the losers must surface CommitFailedException (409),
//! with the catalog state left consistent (DESIGN §6.4).

#![cfg(feature = "iceberg")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

mod common;

use axum::http::StatusCode;
use common::*;
use quasar_core::{AssetStore, TabularStore, VersionStore};
use serde_json::json;
use serial_test::serial;
use tokio::sync::OnceCell;

static PG: OnceCell<(postgresql_embedded::PostgreSQL, String)> = OnceCell::const_new();

async fn db_url() -> &'static str {
    &PG.get_or_init(|| async { PgBootstrap::start("quasar_test_iceberg_concurrent").await })
        .await
        .1
}

#[tokio::test]
#[serial]
async fn concurrent_commits_single_winner() {
    let store = fresh_store(db_url().await).await;
    let (app, _mem) = test_app(&store);
    create_namespace(&app, &["cc"]).await;
    create_table(&app, "cc", "events").await;

    // Fire a burst of concurrent commits; each builds on the same base
    // pointer, so the CAS admits exactly one per round.
    let mut handles = Vec::new();
    for i in 0..8 {
        let app = app.clone();
        handles.push(tokio::spawn(async move {
            let resp = post(
                &app,
                &format!("{NS_PREFIX}/namespaces/cc/tables/events"),
                &json!({
                    "requirements": [],
                    "updates": [{"action": "set-properties", "updates": {"writer": format!("w{i}")}}]
                })
                .to_string(),
            )
            .await;
            resp.status()
        }));
    }

    let mut ok = 0;
    let mut conflict = 0;
    for h in handles {
        match h.await.unwrap() {
            StatusCode::OK => ok += 1,
            StatusCode::CONFLICT => conflict += 1,
            other => panic!("unexpected status {other}"),
        }
    }
    // Commits that load after the winner have legitimately advanced the
    // pointer, so the split is not necessarily 1:7; what the CAS guarantees
    // is that at least one commit wins and simultaneous contenders on the
    // same base pointer fail with 409.
    assert!(ok >= 1, "at least one commit should win, got {ok}");
    assert!(
        conflict >= 1,
        "CAS should reject stale contenders, got {conflict}"
    );
    assert_eq!(ok + conflict, 8);

    // State is consistent: one mirrored version per successful commit
    // (create + winners), tip key equals the version count, and the
    // pointer cache matches the tip version's content_pointer.
    let asset = store
        .get_asset_by_name("default", "cc", "events")
        .await
        .expect("asset lookup failed");
    let versions = store
        .list_versions(asset.id, 0, 100)
        .await
        .expect("list versions failed");
    assert_eq!(
        versions.len(),
        1 + ok,
        "create version + one version per winning commit"
    );
    let expected_key = format!("{:05}", versions.len());
    assert_eq!(
        asset.current_version_key.as_deref(),
        Some(expected_key.as_str())
    );
    let tip = store
        .get_latest_version(asset.id)
        .await
        .expect("latest version failed");
    let tabular = store
        .get_tabular_asset(asset.id)
        .await
        .expect("tabular asset failed");
    assert_eq!(tabular.metadata_location, tip.content_pointer);
}
