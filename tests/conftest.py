import os
import subprocess
import sys
from datetime import datetime, timedelta
from io import BytesIO
from pathlib import Path
from uuid import uuid4

import pyarrow as pa
import pyarrow.parquet as pq
import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[1]
TRIP_SCHEMA = pa.schema(
    [
        ("VendorID", pa.int64()),
        ("tpep_pickup_datetime", pa.timestamp("us")),
        ("tpep_dropoff_datetime", pa.timestamp("us")),
        ("passenger_count", pa.int64()),
        ("trip_distance", pa.float64()),
        ("RatecodeID", pa.int64()),
        ("store_and_fwd_flag", pa.string()),
        ("PULocationID", pa.int64()),
        ("DOLocationID", pa.int64()),
        ("payment_type", pa.int64()),
        ("fare_amount", pa.float64()),
        ("extra", pa.float64()),
        ("mta_tax", pa.float64()),
        ("tip_amount", pa.float64()),
        ("tolls_amount", pa.float64()),
        ("improvement_surcharge", pa.float64()),
        ("total_amount", pa.float64()),
        ("congestion_surcharge", pa.float64()),
        ("Airport_fee", pa.float64()),
    ]
)


class DownloadResponse(BytesIO):
    def __init__(self, payload, advertised_size):
        super().__init__(payload)
        self.headers = {"Content-Length": str(advertised_size)}


def trip(*, month=1, pickup_zone=1, distance=2.0, fare=10.0):
    pickup = datetime(2024, month, 9, 8, 15)
    return {
        "VendorID": 2,
        "tpep_pickup_datetime": pickup,
        "tpep_dropoff_datetime": pickup + timedelta(minutes=20),
        "passenger_count": 1,
        "trip_distance": distance,
        "RatecodeID": 1,
        "store_and_fwd_flag": "N",
        "PULocationID": pickup_zone,
        "DOLocationID": 2,
        "payment_type": 1,
        "fare_amount": fare,
        "extra": 0.0,
        "mta_tax": 0.5,
        "tip_amount": 2.0,
        "tolls_amount": 0.0,
        "improvement_surcharge": 1.0,
        "total_amount": max(fare, 0) + 3.5,
        "congestion_surcharge": 0.0,
        "Airport_fee": 0.0,
    }


@pytest.fixture
def write_trips(tmp_path):
    def write(name, rows, *, drop_column=None):
        target = tmp_path / name
        table = pa.Table.from_pylist(rows, schema=TRIP_SCHEMA)
        if drop_column:
            table = table.drop([drop_column])
        pq.write_table(table, target)
        return target

    return write


@pytest.fixture
def zones_csv(tmp_path):
    target = tmp_path / "taxi_zone_lookup.csv"
    target.write_text(
        'LocationID,Borough,Zone,service_zone\n'
        '1,EWR,Newark Airport,EWR\n'
        '2,Queens,Jamaica Bay,Boro Zone\n'
        '264,Unknown,NV,N/A\n'
        '265,Unknown,Outside of NYC,N/A\n',
        encoding="utf-8",
    )
    return target


@pytest.fixture
def postgres(monkeypatch, tmp_path):
    admin_dsn = os.environ.get("TEST_POSTGRES_DSN")
    if not admin_dsn:
        pytest.skip("Set TEST_POSTGRES_DSN to run PostgreSQL integration tests")

    import psycopg
    from psycopg import sql
    from psycopg.conninfo import conninfo_to_dict

    config = conninfo_to_dict(admin_dsn)
    required = {"host", "port", "user", "password"}
    if missing := required - config.keys():
        pytest.fail(f"TEST_POSTGRES_DSN needs: {', '.join(sorted(missing))}")

    database = f"taxi_test_{uuid4().hex}"
    identifier = sql.Identifier(database)
    settings = {
        "PYTHONPATH": str(PROJECT_ROOT / "src"),
        "PYTHONIOENCODING": "utf-8",
        "PGHOST": config["host"],
        "PGPORT": config["port"],
        "PGUSER": config["user"],
        "PGPASSWORD": config["password"],
        "PGDATABASE": database,
        "DBT_TARGET_PATH": str(tmp_path / "dbt-target"),
        "DBT_LOG_PATH": str(tmp_path / "dbt-logs"),
        "DBT_SEND_ANONYMOUS_USAGE_STATS": "false",
    }
    for name, value in settings.items():
        monkeypatch.setenv(name, value)

    def run(*args, succeeds=True):
        command = [sys.executable, "-m", "taxi_pipeline", *map(str, args)]
        result = subprocess.run(
            command,
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            encoding="utf-8",
            timeout=120,
        )
        output = result.stdout + result.stderr
        assert (result.returncode == 0) == succeeds, output
        return output

    with psycopg.connect(admin_dsn, autocommit=True) as admin:
        admin.execute(sql.SQL("CREATE DATABASE {}").format(identifier))
        try:
            with psycopg.connect(admin_dsn, dbname=database, autocommit=True) as db:
                yield run, db
        finally:
            admin.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(identifier))
