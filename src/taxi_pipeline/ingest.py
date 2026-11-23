import csv
import hashlib
import logging
import math
import shutil
from datetime import UTC, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from time import perf_counter
from urllib.request import Request, urlopen

import pyarrow as pa
import pyarrow.parquet as pq
from psycopg import sql
from psycopg.types.json import Jsonb

from .database import connect, create_raw_tables, lock_warehouse


LOGGER = logging.getLogger(__name__)
TLC_ROOT = "https://d37ci6vzurychx.cloudfront.net"
ZONE_FILENAME = "taxi_zone_lookup.csv"
TRIP_COLUMNS = {
    "vendor_id": "VendorID",
    "pickup_at": "tpep_pickup_datetime",
    "dropoff_at": "tpep_dropoff_datetime",
    "passenger_count": "passenger_count",
    "trip_distance": "trip_distance",
    "rate_code_id": "RatecodeID",
    "store_and_fwd_flag": "store_and_fwd_flag",
    "pickup_location_id": "PULocationID",
    "dropoff_location_id": "DOLocationID",
    "payment_type": "payment_type",
    "fare_amount": "fare_amount",
    "extra": "extra",
    "mta_tax": "mta_tax",
    "tip_amount": "tip_amount",
    "tolls_amount": "tolls_amount",
    "improvement_surcharge": "improvement_surcharge",
    "total_amount": "total_amount",
    "congestion_surcharge": "congestion_surcharge",
    "airport_fee": "Airport_fee",
}
OPTIONAL_TRIP_COLUMNS = {"congestion_surcharge", "airport_fee"}
ZONE_COLUMNS = ("LocationID", "Borough", "Zone", "service_zone")
BATCH_SIZE = 64_000


def download(url: str, destination: Path) -> None:
    request = Request(url, headers={"User-Agent": "nyc-taxi-pipeline/1.0"})
    with urlopen(request, timeout=60) as response, destination.open("wb") as output:
        shutil.copyfileobj(response, output, length=1024 * 1024)
        expected_size = response.headers.get("Content-Length")
        if expected_size is not None and output.tell() != int(expected_size):
            raise ValueError(f"Incomplete download from {url}")


def trip_columns(schema: pa.Schema) -> dict[str, str]:
    names = {name.casefold(): name for name in schema.names}
    if len(names) != len(schema.names):
        raise ValueError("Parquet contains ambiguous column names that differ only by case")
    missing = [name for key, name in TRIP_COLUMNS.items()
               if key not in OPTIONAL_TRIP_COLUMNS and name.casefold() not in names]
    if missing:
        raise ValueError(f"Missing required Parquet columns: {', '.join(missing)}")

    mapping = {key: names[name.casefold()] for key, name in TRIP_COLUMNS.items()
               if name.casefold() in names}
    for key, name in mapping.items():
        data_type = schema.field(name).type
        if pa.types.is_null(data_type):
            continue
        if key in {"pickup_at", "dropoff_at"}:
            compatible = pa.types.is_timestamp(data_type) and data_type.tz is None
        elif key == "store_and_fwd_flag":
            compatible = pa.types.is_string(data_type) or pa.types.is_large_string(data_type)
        else:
            compatible = pa.types.is_integer(data_type) or pa.types.is_floating(data_type)
        if not compatible:
            raise ValueError(f"Incompatible Parquet type for {name}: {data_type}")
    return mapping


def json_safe(value):
    """Keep additive Arrow fields when their Python values need JSON encoding."""
    if isinstance(value, dict):
        return {key: json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_safe(item) for item in value]
    if isinstance(value, float) and not math.isfinite(value):
        return str(value)
    if isinstance(value, bytes):
        return value.hex()
    if value is None or isinstance(value, (str, bool, int, float)):
        return value
    return str(value)


def copy_trips(conn, path, month, filename, loaded_at):
    with pq.ParquetFile(path) as source:
        mapping = trip_columns(source.schema_arrow)
        additional_names = [name for name in source.schema_arrow.names if name not in mapping.values()]
        if source.metadata.num_rows == 0:
            raise ValueError("Parquet file contains no trip records")

        conn.execute("delete from raw.yellow_trips where source_month = %s", (month,))
        target_columns = (
            "source_month", "source_row_number", "source_filename", "loaded_at", *TRIP_COLUMNS,
            "additional_fields",
        )
        copy_statement = sql.SQL("copy raw.yellow_trips ({}) from stdin").format(
            sql.SQL(", ").join(map(sql.Identifier, target_columns))
        )
        row_number = 0
        with conn.cursor() as cursor, cursor.copy(copy_statement) as copy:
            for batch in source.iter_batches(batch_size=BATCH_SIZE):
                columns = [
                    batch.column(mapping[key]).to_pylist() if key in mapping else [None] * batch.num_rows
                    for key in TRIP_COLUMNS
                ]
                additional_rows = (
                    (Jsonb(json_safe(row)) for row in batch.select(additional_names).to_pylist())
                    if additional_names else [None] * batch.num_rows
                )
                for values in zip(*columns, additional_rows, strict=True):
                    row_number += 1
                    copy.write_row((month, row_number, filename, loaded_at, *values))
        if row_number != source.metadata.num_rows:
            raise ValueError(f"Copied {row_number} rows but Parquet advertises {source.metadata.num_rows}")
    conn.execute("analyze raw.yellow_trips")
    return row_number


def copy_zones(conn, path, _month, filename, loaded_at):
    with path.open(encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        missing = set(ZONE_COLUMNS) - set(reader.fieldnames or ())
        if missing:
            raise ValueError(f"Missing required zone CSV columns: {', '.join(sorted(missing))}")
        rows = []
        seen = set()
        for row in reader:
            try:
                location_id = int(row["LocationID"])
                borough = row["Borough"].strip()
                zone = row["Zone"].strip()
                service_zone = row["service_zone"].strip() or None
            except (AttributeError, TypeError, ValueError) as error:
                raise ValueError(f"Malformed zone CSV row at line {reader.line_num}") from error
            if location_id <= 0 or not borough or not zone:
                raise ValueError(f"Missing or invalid zone fields at line {reader.line_num}")
            if location_id in seen:
                raise ValueError(f"Duplicate LocationID {location_id} at line {reader.line_num}")
            seen.add(location_id)
            rows.append((location_id, borough, zone, service_zone, filename, loaded_at))
        if not rows:
            raise ValueError("Zone CSV contains no records")

    conn.execute("delete from raw.taxi_zones")
    with conn.cursor() as cursor, cursor.copy("copy raw.taxi_zones from stdin") as copy:
        for row in rows:
            copy.write_row(row)
    conn.execute("analyze raw.taxi_zones")
    return len(rows)


def load_source(kind, month, official_filename, url, local_file, copy_source):
    started = perf_counter()
    filename = local_file.name if local_file else official_filename
    with connect() as conn:
        lock_warehouse(conn)
        create_raw_tables(conn)
        attempt_id = conn.execute(
            """
            insert into raw.load_attempts (source_kind, source_month, source_filename, status)
            values (%s, %s, %s, 'running') returning attempt_id
            """, (kind, month, filename),
        ).fetchone()[0]
    LOGGER.info("attempt_id=%d source=%s file=%s month=%s result=running", attempt_id, kind, filename, month)

    digest = None
    try:
        with TemporaryDirectory(prefix="taxi-source-") as directory:
            path = Path(directory) / official_filename
            if local_file is None:
                download(url, path)
            else:
                shutil.copyfile(local_file, path)
            with path.open("rb") as handle:
                digest = hashlib.file_digest(handle, "sha256").hexdigest()

            with connect() as conn:
                lock_warehouse(conn)
                row_count = copy_source(conn, path, month, filename, datetime.now(UTC))
                duration = perf_counter() - started
                conn.execute(
                    """
                    update raw.load_attempts
                    set status = 'succeeded', row_count = %s, sha256 = %s, duration_seconds = %s,
                        finished_at = clock_timestamp()
                    where attempt_id = %s
                    """, (row_count, digest, duration, attempt_id),
                )
    except Exception as error:
        duration = perf_counter() - started
        try:
            with connect() as conn:
                conn.execute(
                    """
                    update raw.load_attempts
                    set status = 'failed', sha256 = %s, error_message = %s, duration_seconds = %s,
                        finished_at = clock_timestamp()
                    where attempt_id = %s and status = 'running'
                    """, (digest, str(error).replace("\x00", "\\0")[:4000], duration, attempt_id),
                )
        except Exception:
            LOGGER.exception("Unable to update audit for attempt_id=%d", attempt_id)
        LOGGER.error("attempt_id=%d file=%s duration_seconds=%.3f result=failed", attempt_id, filename, duration)
        raise

    LOGGER.info(
        "attempt_id=%d file=%s rows=%d duration_seconds=%.3f sha256=%s result=succeeded",
        attempt_id, filename, row_count, duration, digest,
    )
    return row_count


def load_trips(month, local_file=None):
    filename = f"yellow_tripdata_{month:%Y-%m}.parquet"
    return load_source("yellow_trips", month, filename, f"{TLC_ROOT}/trip-data/{filename}", local_file, copy_trips)


def load_zones(local_file=None):
    return load_source("taxi_zones", None, ZONE_FILENAME, f"{TLC_ROOT}/misc/{ZONE_FILENAME}", local_file, copy_zones)
