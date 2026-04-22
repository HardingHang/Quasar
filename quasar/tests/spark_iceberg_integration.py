#!/usr/bin/env python3
"""S10: Spark Iceberg REST Catalog end-to-end integration test.

This script can run in two modes:
  1. Host mode (default): Directly on the host machine, accesses localhost:8080/9000
  2. Docker mode: Inside a container, accesses server:8080 and minio:9000

Set RUN_MODE=docker to use Docker mode.

Prerequisites (host mode):
    pip install pyspark==3.5.3 requests boto3

    PySpark will automatically download the Iceberg Spark runtime JAR
    from Maven Central on first use.

Usage:
    # Option 1: Run test inside Docker (no local PySpark needed)
    cd quasar
    docker build -t quasar-server:latest .
    docker compose -f docker-compose.spark.yml up -d
    docker compose -f docker-compose.spark.yml --profile test run --rm spark-test

    # Option 2: Run test on host (requires local PySpark)
    cd quasar
    docker build -t quasar-server:latest .
    docker compose -f docker-compose.spark.yml up -d
    python tests/spark_iceberg_integration.py
"""

import json
import os
import sys
import time

import requests

RUN_MODE = os.environ.get("RUN_MODE", "host")

if RUN_MODE == "docker":
    BASE_URL = os.environ.get("BASE_URL", "http://server:8080")
    MINIO_ENDPOINT = os.environ.get("MINIO_ENDPOINT", "http://minio:9000")
else:
    BASE_URL = "http://localhost:8080"
    MINIO_ENDPOINT = "http://localhost:9000"

QUASAR_TIMEOUT = 30
ICEBERG_CATALOG = "iceberg"


def quasar_post(path: str, payload: dict | None = None) -> dict:
    """POST to Quasar REST API and return JSON response."""
    url = f"{BASE_URL}{path}"
    resp = requests.post(url, json=payload or {}, timeout=QUASAR_TIMEOUT)
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


def quasar_delete(path: str) -> None:
    """DELETE from Quasar REST API."""
    url = f"{BASE_URL}{path}"
    resp = requests.delete(url, timeout=QUASAR_TIMEOUT)
    resp.raise_for_status()


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
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
        region_name="us-east-1",
    )
    try:
        s3.create_bucket(Bucket=bucket)
    except s3.exceptions.BucketAlreadyExists:
        pass
    except Exception as e:
        if "BucketAlreadyOwnedByYou" in str(e):
            pass
        else:
            raise


def build_spark_session():
    """Build a PySpark session with Iceberg REST Catalog configured."""
    from pyspark.sql import SparkSession

    builder = SparkSession.builder.appName("QuasarSparkTest")

    # Iceberg extensions and catalog config
    builder = (
        builder.config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config(
            f"spark.sql.catalog.{ICEBERG_CATALOG}",
            "org.apache.iceberg.spark.SparkCatalog",
        )
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.type", "rest")
        .config(
            f"spark.sql.catalog.{ICEBERG_CATALOG}.uri", f"{BASE_URL}/iceberg"
        )
        .config(
            f"spark.sql.catalog.{ICEBERG_CATALOG}.warehouse", "s3://warehouse/"
        )
    )

    # S3A filesystem config for MinIO
    builder = (
        builder.config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    )

    # Iceberg S3 FileIO config for MinIO
    builder = (
        builder.config(
            f"spark.sql.catalog.{ICEBERG_CATALOG}.s3.endpoint", MINIO_ENDPOINT
        )
        .config(
            f"spark.sql.catalog.{ICEBERG_CATALOG}.s3.access-key-id", "minioadmin"
        )
        .config(
            f"spark.sql.catalog.{ICEBERG_CATALOG}.s3.secret-access-key",
            "minioadmin",
        )
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.s3.region", "us-east-1")
        .config(
            f"spark.sql.catalog.{ICEBERG_CATALOG}.s3.path-style-access", "true"
        )
    )

    # Use Alibaba Maven mirror for faster JAR downloads in China
    builder = builder.config(
        "spark.jars.repositories",
        "https://maven.aliyun.com/repository/central",
    )

    # Auto-download Iceberg Spark runtime and AWS bundle from Maven
    builder = builder.config(
        "spark.jars.packages",
        "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.6.1,org.apache.iceberg:iceberg-aws-bundle:1.6.1",
    )

    return builder.getOrCreate()


def main() -> int:
    print("=" * 50)
    print("Quasar S10: Spark Iceberg Integration Test")
    print(f"Mode: {RUN_MODE}")
    print("=" * 50)

    # 0. Wait for Quasar
    print("\n[Step 0] Waiting for Quasar server...")
    if not wait_for_quasar():
        print("[FAIL] Quasar server did not become ready")
        return 1

    # Create MinIO bucket
    print(f"[Step 0] Creating MinIO bucket 'warehouse' via {MINIO_ENDPOINT}...")
    create_minio_bucket("warehouse")

    passed = 0
    failed = 0

    # Cleanup from previous runs
    print("\n[Cleanup] Removing any leftover namespace from previous runs...")
    try:
        quasar_delete("/iceberg/v1/namespaces/prod")
        print("[Cleanup] Removed leftover namespace 'prod'")
    except Exception:
        pass

    # 1. Create namespace via REST
    print("\n[Step 1] Create namespace: prod")
    try:
        quasar_post("/iceberg/v1/namespaces", {"namespace": ["prod"]})
        if check(True, "Create namespace: prod"):
            passed += 1
        else:
            failed += 1
    except Exception as e:
        check(False, f"Create namespace: prod ({e})")
        failed += 1
        return 1

    # 2. Build Spark session
    print("\n[Step 2] Building Spark session with Iceberg REST Catalog...")
    try:
        spark = build_spark_session()
        if check(True, "Spark session created"):
            passed += 1
        else:
            failed += 1
    except Exception as e:
        check(False, f"Spark session failed ({e})")
        failed += 1
        return 1

    # 3. CREATE TABLE
    print("\n[Step 3] CREATE TABLE iceberg.prod.test (id INT, name STRING)")
    try:
        spark.sql(
            "CREATE TABLE iceberg.prod.test (id INT, name STRING) USING iceberg"
        )
        if check(True, "CREATE TABLE iceberg.prod.test"):
            passed += 1
        else:
            failed += 1
    except Exception as e:
        check(False, f"CREATE TABLE failed ({e})")
        failed += 1
        return 1

    # 4. INSERT INTO
    print(
        "\n[Step 4] INSERT INTO iceberg.prod.test VALUES (1, 'Alice'), (2, 'Bob')"
    )
    try:
        spark.sql(
            "INSERT INTO iceberg.prod.test VALUES (1, 'Alice'), (2, 'Bob')"
        )
        if check(True, "INSERT INTO (2 rows)"):
            passed += 1
        else:
            failed += 1
    except Exception as e:
        check(False, f"INSERT INTO failed ({e})")
        failed += 1
        return 1

    # 5. SELECT * verify
    print("\n[Step 5] SELECT * FROM iceberg.prod.test")
    try:
        df = spark.sql("SELECT * FROM iceberg.prod.test")
        row_count = df.count()
        columns = df.columns

        if check(row_count == 2, f"SELECT * returned {row_count} rows"):
            passed += 1
        else:
            failed += 1

        if check(
            set(columns) == {"id", "name"},
            f"SELECT * columns: {columns}",
        ):
            passed += 1
        else:
            failed += 1
    except Exception as e:
        check(False, f"SELECT * failed ({e})")
        failed += 2
        return 1

    # 6. Append more data
    print("\n[Step 6] INSERT INTO iceberg.prod.test VALUES (3, 'Charlie')")
    try:
        spark.sql("INSERT INTO iceberg.prod.test VALUES (3, 'Charlie')")
        df2 = spark.sql("SELECT * FROM iceberg.prod.test")
        row_count2 = df2.count()

        if check(row_count2 == 3, f"After append: {row_count2} rows"):
            passed += 1
        else:
            failed += 1
    except Exception as e:
        check(False, f"Append data failed ({e})")
        failed += 1
        return 1

    # 7. DROP TABLE
    print("\n[Step 7] DROP TABLE iceberg.prod.test")
    try:
        spark.sql("DROP TABLE iceberg.prod.test")
        if check(True, "DROP TABLE iceberg.prod.test"):
            passed += 1
        else:
            failed += 1
    except Exception as e:
        check(False, f"DROP TABLE failed ({e})")
        failed += 1
        return 1

    # 8. Cleanup namespace via REST
    print("\n[Step 8] Cleanup: drop namespace 'prod'")
    try:
        quasar_delete("/iceberg/v1/namespaces/prod")
        if check(True, "Drop namespace: prod"):
            passed += 1
        else:
            failed += 1
    except Exception as e:
        check(False, f"Drop namespace failed ({e})")
        failed += 1

    # Summary
    print("\n" + "=" * 50)
    print(f"Results: {passed} passed, {failed} failed")
    print("=" * 50)

    spark.stop()
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
