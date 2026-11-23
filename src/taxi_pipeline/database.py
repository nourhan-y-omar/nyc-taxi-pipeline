import os

import psycopg


# Shared by ingestion and the dbt launcher to serialize writes to the warehouse.
WAREHOUSE_LOCK = 4_240_024


def connect():
    return psycopg.connect(
        host=os.environ.get("PGHOST", "localhost"),
        port=os.environ.get("PGPORT", "5432"),
        dbname=os.environ.get("PGDATABASE", "taxi"),
        user=os.environ.get("PGUSER", "taxi"),
        connect_timeout=10,
    )


def lock_warehouse(conn):
    conn.execute("select pg_advisory_xact_lock(%s)", (WAREHOUSE_LOCK,))


def create_raw_tables(conn):
    conn.execute("create schema if not exists raw")
    conn.execute("""
        create table if not exists raw.load_attempts (
            attempt_id bigint generated always as identity primary key,
            source_kind text not null check (source_kind in ('yellow_trips', 'taxi_zones')),
            source_month date,
            source_filename text not null,
            status text not null check (status in ('running', 'succeeded', 'failed')),
            row_count bigint,
            sha256 text,
            error_message text,
            duration_seconds double precision,
            started_at timestamptz not null default clock_timestamp(),
            finished_at timestamptz,
            check ((source_kind = 'yellow_trips') = (source_month is not null))
        )
        """)
    conn.execute("""
        create table if not exists raw.yellow_trips (
            source_month date not null,
            source_row_number bigint not null check (source_row_number > 0),
            source_filename text not null,
            loaded_at timestamptz not null,
            vendor_id double precision,
            pickup_at timestamp without time zone,
            dropoff_at timestamp without time zone,
            passenger_count double precision,
            trip_distance double precision,
            rate_code_id double precision,
            store_and_fwd_flag text,
            pickup_location_id double precision,
            dropoff_location_id double precision,
            payment_type double precision,
            fare_amount double precision,
            extra double precision,
            mta_tax double precision,
            tip_amount double precision,
            tolls_amount double precision,
            improvement_surcharge double precision,
            total_amount double precision,
            congestion_surcharge double precision,
            airport_fee double precision,
            additional_fields jsonb,
            primary key (source_month, source_row_number)
        )
        """)
    conn.execute("""
        create table if not exists raw.taxi_zones (
            location_id integer primary key,
            borough text not null,
            zone text not null,
            service_zone text,
            source_filename text not null,
            loaded_at timestamptz not null
        )
        """)
