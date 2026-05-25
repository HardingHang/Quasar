#!/usr/bin/env python3
"""S10: Spark Iceberg REST Catalog end-to-end integration test.

V4.0 C6: Spark 3.5 + Iceberg 1.10.x E2E validation.

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

# V4.0 target: Iceberg 1.10.x
ICEBERG_VERSION = "1.10.1"


class TestRunner:
    """Collects PASS/FAIL results across all test steps."""

    def __init__(self):
        self.passed = 0
        self.failed = 0
        self.spark = None

    def check(self, condition: bool, label: str) -> bool:
        if condition:
            print(f"[PASS] {label}")
            self.passed += 1
            return True
        else:
            print(f"[FAIL] {label}")
            self.failed += 1
            return False

    def summary(self) -> int:
        print("\n" + "=" * 50)
        print(f"Results: {self.passed} passed, {self.failed} failed")
        print("=" * 50)
        return 0 if self.failed == 0 else 1


# ── REST helpers ───────────────────────────────────────────


def quasar_post(path: str, payload: dict | None = None) -> dict:
    url = f"{BASE_URL}{path}"
    resp = requests.post(url, json=payload or {}, timeout=QUASAR_TIMEOUT)
    resp.raise_for_status()
    if not resp.text:
        return {}
    return resp.json()


def quasar_get(path: str) -> dict:
    url = f"{BASE_URL}{path}"
    resp = requests.get(url, timeout=QUASAR_TIMEOUT)
    resp.raise_for_status()
    return resp.json()


def quasar_delete(path: str) -> None:
    url = f"{BASE_URL}{path}"
    resp = requests.delete(url, timeout=QUASAR_TIMEOUT)
    resp.raise_for_status()


def quasar_head(path: str) -> int:
    url = f"{BASE_URL}{path}"
    resp = requests.head(url, timeout=QUASAR_TIMEOUT)
    return resp.status_code


def wait_for_quasar(max_wait: int = 60) -> bool:
    for _ in range(max_wait):
        try:
            resp = requests.get(f"{BASE_URL}/healthz", timeout=2)
            if resp.status_code == 200:
                return True
        except requests.RequestException:
            pass
        time.sleep(1)
    return False


# ── MinIO helpers ──────────────────────────────────────────


def create_minio_bucket(bucket: str) -> None:
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


# ── Spark session builder ──────────────────────────────────


def build_spark_session():
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
        .config(f"spark.sql.catalog.{ICEBERG_CATALOG}.prefix", "default")
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

    # Force HTTP/1.1 to avoid HTTP/2 compatibility issues with h2c
    # Also set AWS region for iceberg-aws-bundle SDK v2
    builder = builder.config(
        "spark.driver.extraJavaOptions",
        "-Djdk.httpclient.HttpClient.version=HTTP_1_1 -Daws.region=us-east-1",
    )
    builder = builder.config(
        "spark.executor.extraJavaOptions",
        "-Djdk.httpclient.HttpClient.version=HTTP_1_1 -Daws.region=us-east-1",
    )

    # Use Alibaba Maven mirror for faster JAR downloads in China
    builder = builder.config(
        "spark.jars.repositories",
        "https://maven.aliyun.com/repository/central",
    )

    # Auto-download Iceberg Spark runtime and AWS bundle from Maven
    packages = (
        f"org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:{ICEBERG_VERSION},"
        f"org.apache.iceberg:iceberg-aws-bundle:{ICEBERG_VERSION}"
    )
    builder = builder.config("spark.jars.packages", packages)

    return builder.getOrCreate()


# ── Test steps ─────────────────────────────────────────────


def step_0_setup(runner: TestRunner) -> bool:
    print("\n[Step 0] Waiting for Quasar server...")
    if not runner.check(wait_for_quasar(), "Quasar server ready"):
        return False

    print(f"[Step 0] Creating MinIO bucket 'warehouse' via {MINIO_ENDPOINT}...")
    try:
        create_minio_bucket("warehouse")
        runner.check(True, "MinIO bucket 'warehouse' ready")
    except Exception as e:
        runner.check(False, f"MinIO bucket creation failed: {e}")
        return False

    # Cleanup from previous runs
    print("[Cleanup] Removing any leftover namespace from previous runs...")
    try:
        quasar_delete("/iceberg/v1/default/namespaces/prod")
        print("[Cleanup] Removed leftover namespace 'prod'")
    except Exception:
        pass

    return True


def step_1_create_namespace(runner: TestRunner) -> bool:
    print("\n[Step 1] Create namespace: prod")
    try:
        quasar_post("/iceberg/v1/default/namespaces", {"namespace": ["prod"]})
        return runner.check(True, "Create namespace: prod")
    except Exception as e:
        return runner.check(False, f"Create namespace failed: {e}")


def step_2_build_spark(runner: TestRunner) -> bool:
    print("\n[Step 2] Building Spark session with Iceberg REST Catalog...")
    try:
        runner.spark = build_spark_session()
        return runner.check(True, f"Spark session created (Iceberg {ICEBERG_VERSION})")
    except Exception as e:
        return runner.check(False, f"Spark session failed: {e}")


def step_3_create_table(runner: TestRunner) -> bool:
    print("\n[Step 3] CREATE TABLE iceberg.prod.test (id INT, name STRING)")
    try:
        runner.spark.sql(
            "CREATE TABLE iceberg.prod.test (id INT, name STRING) USING iceberg"
        )
        return runner.check(True, "CREATE TABLE iceberg.prod.test")
    except Exception as e:
        return runner.check(False, f"CREATE TABLE failed: {e}")


def step_4_create_table_if_not_exists(runner: TestRunner) -> bool:
    print("\n[Step 4] CREATE TABLE IF NOT EXISTS iceberg.prod.test")
    try:
        runner.spark.sql(
            "CREATE TABLE IF NOT EXISTS iceberg.prod.test (id INT, name STRING) USING iceberg"
        )
        return runner.check(True, "CREATE TABLE IF NOT EXISTS (idempotent)")
    except Exception as e:
        return runner.check(False, f"CREATE TABLE IF NOT EXISTS failed: {e}")


def step_5_dataframe_write_to_create(runner: TestRunner) -> bool:
    print("\n[Step 5] DataFrame .writeTo(...).create() for iceberg.prod.df_table")
    try:
        df = runner.spark.createDataFrame(
            [(1, "alpha"), (2, "beta")],
            ["id", "name"],
        )
        df.writeTo("iceberg.prod.df_table").create()

        # Verify
        result = runner.spark.sql("SELECT * FROM iceberg.prod.df_table").collect()
        return runner.check(
            len(result) == 2,
            f"DataFrame writeTo create: {len(result)} rows",
        )
    except Exception as e:
        return runner.check(False, f"DataFrame writeTo create failed: {e}")


def step_6_insert_into(runner: TestRunner) -> bool:
    print("\n[Step 6] INSERT INTO iceberg.prod.test VALUES (1, 'Alice'), (2, 'Bob')")
    try:
        runner.spark.sql(
            "INSERT INTO iceberg.prod.test VALUES (1, 'Alice'), (2, 'Bob')"
        )
        return runner.check(True, "INSERT INTO (2 rows)")
    except Exception as e:
        return runner.check(False, f"INSERT INTO failed: {e}")


def step_7_select_verify(runner: TestRunner) -> bool:
    print("\n[Step 7] SELECT * FROM iceberg.prod.test")
    try:
        df = runner.spark.sql("SELECT * FROM iceberg.prod.test")
        row_count = df.count()
        columns = df.columns

        ok = runner.check(row_count == 2, f"SELECT * returned {row_count} rows")
        ok &= runner.check(
            set(columns) == {"id", "name"},
            f"SELECT * columns: {columns}",
        )
        return ok
    except Exception as e:
        runner.check(False, f"SELECT * failed: {e}")
        return False


def step_8_dataframe_append(runner: TestRunner) -> bool:
    print("\n[Step 8] DataFrame .writeTo(...).append()")
    try:
        df = runner.spark.createDataFrame([(3, "Charlie")], ["id", "name"])
        df.writeTo("iceberg.prod.test").append()

        df2 = runner.spark.sql("SELECT * FROM iceberg.prod.test")
        row_count = df2.count()
        return runner.check(row_count == 3, f"After DataFrame append: {row_count} rows")
    except Exception as e:
        return runner.check(False, f"DataFrame append failed: {e}")


def step_9_schema_evolution(runner: TestRunner) -> bool:
    print("\n[Step 9] Schema evolution: ADD / DROP / RENAME / ALTER COLUMN")
    ok = True
    try:
        # ADD COLUMN
        runner.spark.sql(
            "ALTER TABLE iceberg.prod.test ADD COLUMN age INT"
        )
        ok &= runner.check(True, "ADD COLUMN age INT")

        # Insert with new column
        runner.spark.sql(
            "INSERT INTO iceberg.prod.test VALUES (4, 'David', 30)"
        )
        df = runner.spark.sql("SELECT * FROM iceberg.prod.test WHERE id = 4")
        ok &= runner.check(
            df.collect()[0]["age"] == 30,
            "ADD COLUMN data accessible",
        )

        # RENAME COLUMN
        runner.spark.sql(
            "ALTER TABLE iceberg.prod.test RENAME COLUMN age TO years_old"
        )
        df = runner.spark.sql("SELECT years_old FROM iceberg.prod.test WHERE id = 4")
        ok &= runner.check(
            df.collect()[0]["years_old"] == 30,
            "RENAME COLUMN accessible",
        )

        # ALTER COLUMN (set nullability / type)
        runner.spark.sql(
            "ALTER TABLE iceberg.prod.test ALTER COLUMN years_old DROP NOT NULL"
        )
        ok &= runner.check(True, "ALTER COLUMN DROP NOT NULL")

        # DROP COLUMN
        runner.spark.sql(
            "ALTER TABLE iceberg.prod.test DROP COLUMN years_old"
        )
        cols = runner.spark.sql("SELECT * FROM iceberg.prod.test").columns
        ok &= runner.check(
            "years_old" not in cols,
            f"DROP COLUMN removed (cols: {cols})",
        )
    except Exception as e:
        ok &= runner.check(False, f"Schema evolution failed: {e}")
    return ok


def step_10_partition_transform(runner: TestRunner) -> bool:
    print("\n[Step 10] Partition transform: identity, days(), bucket()")
    ok = True
    try:
        # Create a table with timestamp column for partition transforms
        runner.spark.sql(
            "CREATE TABLE iceberg.prod.events ("
            "  event_id INT,"
            "  event_time TIMESTAMP,"
            "  user_id STRING"
            ") USING iceberg PARTITIONED BY (days(event_time))"
        )
        ok &= runner.check(True, "CREATE TABLE with days() partition")

        # Insert data
        runner.spark.sql(
            "INSERT INTO iceberg.prod.events VALUES "
            "(1, TIMESTAMP '2024-01-15 10:00:00', 'user1'),"
            "(2, TIMESTAMP '2024-01-16 11:00:00', 'user2')"
        )

        # Verify data
        df = runner.spark.sql("SELECT * FROM iceberg.prod.events")
        ok &= runner.check(df.count() == 2, f"Partitioned table rows: {df.count()}")

        # Add identity partition field
        runner.spark.sql(
            "ALTER TABLE iceberg.prod.events ADD PARTITION FIELD user_id"
        )
        ok &= runner.check(True, "ADD PARTITION FIELD identity(user_id)")

        # Add bucket partition field
        runner.spark.sql(
            "ALTER TABLE iceberg.prod.events ADD PARTITION FIELD bucket(16, event_id)"
        )
        ok &= runner.check(True, "ADD PARTITION FIELD bucket(16, event_id)")

        # Cleanup
        runner.spark.sql("DROP TABLE iceberg.prod.events")
        ok &= runner.check(True, "DROP partitioned table")
    except Exception as e:
        ok &= runner.check(False, f"Partition transform failed: {e}")
    return ok


def step_11_snapshot_read(runner: TestRunner) -> bool:
    print("\n[Step 11] Snapshot read (time travel)")
    ok = True
    try:
        # Get current snapshot id
        snapshots_df = runner.spark.sql(
            "SELECT snapshot_id FROM iceberg.prod.test.snapshots"
        )
        snapshot_ids = [row.snapshot_id for row in snapshots_df.collect()]
        ok &= runner.check(
            len(snapshot_ids) >= 2,
            f"Table has {len(snapshot_ids)} snapshots",
        )

        if len(snapshot_ids) >= 2:
            first_snapshot = snapshot_ids[0]
            df = runner.spark.sql(
                f"SELECT * FROM iceberg.prod.test VERSION AS OF {first_snapshot}"
            )
            ok &= runner.check(
                df.count() >= 2,
                f"Time travel to snapshot {first_snapshot}: {df.count()} rows",
            )
    except Exception as e:
        ok &= runner.check(False, f"Snapshot read failed: {e}")
    return ok


def step_12_branch_tag(runner: TestRunner) -> bool:
    print("\n[Step 12] Create / drop branch and tag")
    ok = True
    try:
        # Create branch
        runner.spark.sql(
            "ALTER TABLE iceberg.prod.test CREATE BRANCH test_branch"
        )
        ok &= runner.check(True, "CREATE BRANCH test_branch")

        # Create tag
        runner.spark.sql(
            "ALTER TABLE iceberg.prod.test CREATE TAG test_tag"
        )
        ok &= runner.check(True, "CREATE TAG test_tag")

        # Verify branch exists via refs
        refs_df = runner.spark.sql("SELECT * FROM iceberg.prod.test.refs")
        ref_names = [row.name for row in refs_df.collect()]
        ok &= runner.check(
            "test_branch" in ref_names,
            f"Branch exists in refs: {ref_names}",
        )
        ok &= runner.check(
            "test_tag" in ref_names,
            f"Tag exists in refs: {ref_names}",
        )

        # Drop branch
        runner.spark.sql(
            "ALTER TABLE iceberg.prod.test DROP BRANCH test_branch"
        )
        ok &= runner.check(True, "DROP BRANCH test_branch")

        # Drop tag
        runner.spark.sql(
            "ALTER TABLE iceberg.prod.test DROP TAG test_tag"
        )
        ok &= runner.check(True, "DROP TAG test_tag")
    except Exception as e:
        ok &= runner.check(False, f"Branch/tag failed: {e}")
    return ok


def step_13_expire_snapshots(runner: TestRunner) -> bool:
    print("\n[Step 13] expire_snapshots smoke")
    ok = True
    try:
        before = runner.spark.sql(
            "SELECT * FROM iceberg.prod.test.snapshots"
        ).count()

        runner.spark.sql(
            "CALL iceberg.system.expire_snapshots("
            "  table => 'iceberg.prod.test',"
            "  older_than => TIMESTAMP '2099-01-01 00:00:00.000',"
            "  retain_last => 1"
            ")"
        )

        after = runner.spark.sql(
            "SELECT * FROM iceberg.prod.test.snapshots"
        ).count()
        ok &= runner.check(
            after <= before,
            f"expire_snapshots: snapshots {before} -> {after}",
        )
    except Exception as e:
        ok &= runner.check(False, f"expire_snapshots failed: {e}")
    return ok


def step_14_metadata_compatibility(runner: TestRunner) -> bool:
    print("\n[Step 14] Metadata compatibility: Spark reads Quasar metadata")
    ok = True
    try:
        # Read metadata via DESCRIBE EXTENDED Table Properties
        desc_df = runner.spark.sql("DESCRIBE EXTENDED iceberg.prod.test")
        props = {}
        for row in desc_df.collect():
            if row.col_name == "Table Properties":
                # Parse: [key=value,key2=value2,...]
                prop_str = row.data_type.strip("[]")
                for item in prop_str.split(","):
                    if "=" in item:
                        k, v = item.split("=", 1)
                        props[k] = v

        ok &= runner.check(
            props.get("format-version") == "2",
            f"Metadata format_version: {props.get('format-version')}",
        )
        ok &= runner.check(
            props.get("table-uuid") is not None,
            f"Metadata table_uuid present: {props.get('table-uuid')}",
        )

        # Verify current snapshot
        snapshots_df = runner.spark.sql(
            "SELECT * FROM iceberg.prod.test.snapshots"
        )
        snapshots = snapshots_df.collect()
        ok &= runner.check(
            len(snapshots) >= 1,
            f"Snapshots count: {len(snapshots)}",
        )

        # Verify partition spec
        partitions_df = runner.spark.sql(
            "SELECT * FROM iceberg.prod.test.partitions"
        )
        ok &= runner.check(True, "Partitions readable")

        # Verify schema via DESCRIBE
        schema_df = runner.spark.sql("DESCRIBE iceberg.prod.test")
        cols = [row.col_name for row in schema_df.collect()]
        ok &= runner.check(
            "id" in cols and "name" in cols,
            f"Schema columns: {cols}",
        )
    except Exception as e:
        ok &= runner.check(False, f"Metadata compatibility failed: {e}")
    return ok


def step_15_drop_table_purge(runner: TestRunner) -> bool:
    print("\n[Step 15] DROP TABLE and DROP TABLE PURGE")
    ok = True
    try:
        # DROP TABLE (no purge)
        runner.spark.sql("DROP TABLE iceberg.prod.df_table")
        ok &= runner.check(True, "DROP TABLE iceberg.prod.df_table")

        # DROP TABLE PURGE
        runner.spark.sql("DROP TABLE iceberg.prod.test PURGE")
        ok &= runner.check(True, "DROP TABLE iceberg.prod.test PURGE")
    except Exception as e:
        ok &= runner.check(False, f"DROP TABLE failed: {e}")
    return ok


def step_16_cleanup_namespace(runner: TestRunner) -> bool:
    print("\n[Step 16] Cleanup: drop namespace 'prod'")
    try:
        quasar_delete("/iceberg/v1/default/namespaces/prod")
        return runner.check(True, "Drop namespace: prod")
    except Exception as e:
        return runner.check(False, f"Drop namespace failed: {e}")


# ── Main ───────────────────────────────────────────────────


def main() -> int:
    print("=" * 50)
    print("Quasar V4.0 C6: Spark Iceberg Integration Test")
    print(f"Mode: {RUN_MODE}  |  Iceberg: {ICEBERG_VERSION}")
    print("=" * 50)

    runner = TestRunner()

    steps = [
        ("Setup", step_0_setup),
        ("Create namespace", step_1_create_namespace),
        ("Build Spark session", step_2_build_spark),
        ("CREATE TABLE", step_3_create_table),
        ("CREATE TABLE IF NOT EXISTS", step_4_create_table_if_not_exists),
        ("DataFrame writeTo create", step_5_dataframe_write_to_create),
        ("INSERT INTO", step_6_insert_into),
        ("SELECT verify", step_7_select_verify),
        ("DataFrame append", step_8_dataframe_append),
        ("Schema evolution", step_9_schema_evolution),
        ("Partition transform", step_10_partition_transform),
        ("Snapshot read", step_11_snapshot_read),
        ("Branch/tag", step_12_branch_tag),
        ("expire_snapshots", step_13_expire_snapshots),
        ("Metadata compatibility", step_14_metadata_compatibility),
        ("DROP TABLE / PURGE", step_15_drop_table_purge),
        ("Cleanup namespace", step_16_cleanup_namespace),
    ]

    for label, step_fn in steps:
        if not step_fn(runner):
            print(f"\n[ABORT] Step '{label}' failed. Stopping.")
            break

    if runner.spark:
        runner.spark.stop()

    return runner.summary()


if __name__ == "__main__":
    sys.exit(main())
