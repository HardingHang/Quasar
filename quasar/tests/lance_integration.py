#!/usr/bin/env python3
"""S7: Lance Python SDK end-to-end integration test.

Prerequisites:
    pip install requests pyarrow lance boto3

Usage:
    docker-compose -f docker-compose.lance.yml up -d
    python tests/lance_integration.py
"""

import json
import sys
import time
import urllib.parse

import requests

BASE_URL = "http://localhost:8080"
QUASAR_TIMEOUT = 30


def quasar_post(path: str, payload: dict | None = None) -> dict:
    """POST to Quasar REST API and return JSON response."""
    url = f"{BASE_URL}{path}"
    resp = requests.post(
        url,
        json=payload or {},
        timeout=QUASAR_TIMEOUT,
    )
    resp.raise_for_status()
    if not resp.text:
        return {}
    return resp.json()


def quasar_get(path: str) -> dict:
    """GET from Quasar REST API and return JSON response."""
    url = f"{BASE_URL}{path}"
    resp = requests.get(url, timeout=QUASAR_TIMEOUT)
    resp.raise_for_status()
    return resp.json()


def check(condition: bool, label: str) -> bool:
    """Print PASS/FAIL for a check."""
    if condition:
        print(f"[PASS] {label}")
        return True
    else:
        print(f"[FAIL] {label}")
        return False


def wait_for_quasar(max_wait: int = 60) -> bool:
    """Wait until Quasar /healthz returns 200."""
    for _ in range(max_wait):
        try:
            resp = requests.get(f"{BASE_URL}/healthz", timeout=2)
            if resp.status_code == 200:
                return True
        except requests.RequestException:
            pass
        time.sleep(1)
    return False


def create_minio_bucket(bucket: str) -> None:
    """Create S3 bucket in MinIO (idempotent)."""
    import boto3

    s3 = boto3.client(
        "s3",
        endpoint_url="http://localhost:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
        region_name="us-east-1",
    )
    try:
        s3.create_bucket(Bucket=bucket)
    except s3.exceptions.BucketAlreadyExists:
        pass
    except Exception as e:
        # Some MinIO versions throw ClientError with BucketAlreadyOwnedByYou
        if "BucketAlreadyOwnedByYou" in str(e):
            pass
        else:
            raise


def main() -> int:
    print("=" * 50)
    print("Quasar S7: Lance Integration Test")
    print("=" * 50)

    # 0. Wait for Quasar
    print("\n[Step 0] Waiting for Quasar server...")
    if not wait_for_quasar():
        print("[FAIL] Quasar server did not become ready")
        return 1

    # Create MinIO bucket
    print("[Step 0] Creating MinIO bucket 'warehouse'...")
    create_minio_bucket("warehouse")

    passed = 0
    failed = 0

    # 1. Create namespace
    print("\n[Step 1] Create namespace: prod")
    try:
        quasar_post("/lance/v1/namespace/prod/create")
        if check(True, "Create namespace: prod"):
            passed += 1
        else:
            failed += 1
    except Exception as e:
        check(False, f"Create namespace: prod ({e})")
        failed += 1

    # 2. Declare table
    print("\n[Step 2] Declare table: prod.users")
    try:
        table_id = urllib.parse.quote("prod$users", safe="")
        declare_resp = quasar_post(f"/lance/v1/table/{table_id}/declare")
        location = declare_resp.get("location", "")
        storage_options = declare_resp.get("storage_options", {})
        # Replace internal Docker endpoint with host-accessible endpoint
        if storage_options.get("endpoint") == "http://minio:9000":
            storage_options["endpoint"] = "http://localhost:9000"

        expected_location = "s3://warehouse/prod/users/"
        if check(
            location == expected_location,
            f"Declare table: location={location}",
        ):
            passed += 1
        else:
            failed += 1

        if check(
            bool(storage_options.get("endpoint")),
            "Declare table: storage_options contains endpoint",
        ):
            passed += 1
        else:
            failed += 1
    except Exception as e:
        check(False, f"Declare table: prod.users ({e})")
        failed += 2
        return 1

    # 3. Write Arrow data to location using Lance Python SDK
    print("\n[Step 3] Write data to location (3 rows)")
    try:
        import lance
        import pyarrow as pa

        table_v1 = pa.table({
            "id": [1, 2, 3],
            "name": ["alice", "bob", "charlie"],
        })

        # write_dataset returns a LanceDataset; read version directly
        # without re-opening, avoiding extra S3 HEAD/GET round-trips.
        dataset = lance.write_dataset(
            table_v1,
            location,
            storage_options=storage_options,
        )
        actual_version_v1 = dataset.version

        if check(True, f"Write data to location (3 rows), Lance version={actual_version_v1}"):
            passed += 1
        else:
            failed += 1
    except Exception as e:
        check(False, f"Write data to location ({e})")
        failed += 1
        return 1

    # 4. Create table version v1
    print("\n[Step 4] Create table version: v1")
    try:
        manifest_path = f"{location}_versions/{actual_version_v1}.manifest"
        quasar_post(
            f"/lance/v1/table/{table_id}/version/create",
            {"version": actual_version_v1, "manifest_path": manifest_path},
        )
        if check(True, f"Create table version: v1 (Lance version={actual_version_v1})"):
            passed += 1
        else:
            failed += 1
    except Exception as e:
        check(False, f"Create table version: v1 ({e})")
        failed += 1

    # 5. Describe table
    print("\n[Step 5] Describe table: verify current_version=1")
    try:
        describe_resp = quasar_post(f"/lance/v1/table/{table_id}/describe")
        current_version = describe_resp.get("current_version")

        if check(
            current_version == 1,
            f"Describe table: current_version={current_version}",
        ):
            passed += 1
        else:
            failed += 1
    except Exception as e:
        check(False, f"Describe table ({e})")
        failed += 1

    # 6. Read data back using Lance Python SDK
    print("\n[Step 6] Read data back using Lance SDK")
    try:
        ds = lance.dataset(location, storage_options=storage_options)
        df = ds.to_table().to_pandas()

        row_count_ok = len(df) == 3
        schema_ok = list(df.columns) == ["id", "name"]

        if check(row_count_ok, f"Read data back: {len(df)} rows"):
            passed += 1
        else:
            failed += 1

        if check(schema_ok, f"Read data back: schema={list(df.columns)}"):
            passed += 1
        else:
            failed += 1
    except Exception as e:
        check(False, f"Read data back ({e})")
        failed += 2

    # 7. Write second batch (5 rows total)
    print("\n[Step 7] Write second batch (5 rows total)")
    try:
        table_v2 = pa.table({
            "id": [1, 2, 3, 4, 5],
            "name": ["alice", "bob", "charlie", "diana", "eve"],
        })

        # write_dataset returns a LanceDataset; read version directly
        # without re-opening, avoiding extra S3 HEAD/GET round-trips.
        dataset = lance.write_dataset(
            table_v2,
            location,
            mode="append",
            storage_options=storage_options,
        )
        actual_version_v2 = dataset.version

        if check(True, f"Write second batch (5 rows total), Lance version={actual_version_v2}"):
            passed += 1
        else:
            failed += 1
    except Exception as e:
        check(False, f"Write second batch ({e})")
        failed += 1

    # 8. Create table version v2
    print("\n[Step 8] Create table version: v2")
    try:
        manifest_path_v2 = f"{location}_versions/{actual_version_v2}.manifest"
        quasar_post(
            f"/lance/v1/table/{table_id}/version/create",
            {"version": actual_version_v2, "manifest_path": manifest_path_v2},
        )
        if check(True, f"Create table version: v2 (Lance version={actual_version_v2})"):
            passed += 1
        else:
            failed += 1
    except Exception as e:
        check(False, f"Create table version: v2 ({e})")
        failed += 1

    # 9. List versions
    print("\n[Step 9] List versions: verify [1, 2]")
    try:
        versions_resp = quasar_get(f"/lance/v1/table/{table_id}/version/list")
        versions = versions_resp.get("versions", [])

        if check(
            versions == [1, 2],
            f"List versions: {versions}",
        ):
            passed += 1
        else:
            failed += 1
    except Exception as e:
        check(False, f"List versions ({e})")
        failed += 1

    # 10. Cleanup
    print("\n[Step 10] Cleanup: drop table and namespace")
    try:
        quasar_post(f"/lance/v1/table/{table_id}/drop")
        quasar_post("/lance/v1/namespace/prod/drop")
        if check(True, "Drop table + namespace"):
            passed += 1
        else:
            failed += 1
    except Exception as e:
        check(False, f"Cleanup ({e})")
        failed += 1

    # Summary
    print("\n" + "=" * 50)
    print(f"Results: {passed} passed, {failed} failed")
    print("=" * 50)

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
