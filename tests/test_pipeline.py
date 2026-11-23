from concurrent.futures import ThreadPoolExecutor
from datetime import date
from hashlib import sha256
from urllib.error import HTTPError

from conftest import DownloadResponse, trip
from taxi_pipeline import ingest
from taxi_pipeline.__main__ import main


JANUARY = date(2024, 1, 1)
FEBRUARY = date(2024, 2, 1)


def trip_snapshot(db):
    return db.execute(
        """
        select source_month, source_row_number, source_filename, fare_amount
        from raw.yellow_trips
        order by source_month, source_row_number
        """
    ).fetchall()


def test_rerun_and_correction_replace_only_the_source_month(postgres, write_trips):
    run, db = postgres
    january = write_trips("january.parquet", [trip(), trip()])
    february = write_trips("february.parquet", [trip(month=2, fare=20)])

    run("load-trips", "--year", 2024, "--month", 1, "--file", january)
    original = trip_snapshot(db)
    assert original == [
        (JANUARY, 1, january.name, 10),
        (JANUARY, 2, january.name, 10),
    ]

    run("load-trips", "--year", 2024, "--month", 1, "--file", january)
    assert trip_snapshot(db) == original

    run("load-trips", "--year", 2024, "--month", 2, "--file", february)
    february_loaded_at = db.execute(
        "select loaded_at from raw.yellow_trips where source_month = %s", (FEBRUARY,)
    ).fetchone()[0]

    corrected = write_trips(january.name, [trip(fare=12)])
    run("load-trips", "--year", 2024, "--month", 1, "--file", corrected)
    assert trip_snapshot(db) == [
        (JANUARY, 1, january.name, 12),
        (FEBRUARY, 1, february.name, 20),
    ]
    assert db.execute(
        "select loaded_at from raw.yellow_trips where source_month = %s", (FEBRUARY,)
    ).fetchone()[0] == february_loaded_at


def test_concurrent_first_loads_leave_one_complete_source_file(postgres, write_trips):
    run, db = postgres
    first = write_trips("first.parquet", [trip(), trip()])
    second = write_trips("second.parquet", [trip(fare=15)])
    load = ("load-trips", "--year", 2024, "--month", 1, "--file")

    with ThreadPoolExecutor(max_workers=2) as pool:
        loads = [pool.submit(run, *load, path) for path in (first, second)]
        for result in loads:
            result.result()

    assert trip_snapshot(db) in (
        [(JANUARY, 1, first.name, 10), (JANUARY, 2, first.name, 10)],
        [(JANUARY, 1, second.name, 15)],
    )
    assert db.execute(
        """
        select source_filename, row_count
        from raw.load_attempts
        where status = 'succeeded' and source_month = %s
        order by source_filename
        """,
        (JANUARY,),
    ).fetchall() == [(first.name, 2), (second.name, 1)]


def test_schema_failure_and_insert_failure_leave_prior_data_and_allow_retry(
    postgres, write_trips
):
    run, db = postgres
    january = write_trips("january.parquet", [trip()])
    load = ("load-trips", "--year", 2024, "--month", 1, "--file")
    run(*load, january)
    original = trip_snapshot(db)

    bad_schema = write_trips("missing_fare.parquet", [trip()], drop_column="fare_amount")
    assert "fare_amount" in run(*load, bad_schema, succeeds=False)
    assert trip_snapshot(db) == original

    db.execute(
        """
        alter table raw.yellow_trips add constraint test_reject_second_row
        check (source_row_number = 1)
        """
    )
    replacement = write_trips(january.name, [trip(fare=15), trip(fare=18)])
    assert "test_reject_second_row" in run(*load, replacement, succeeds=False)
    assert trip_snapshot(db) == original
    db.execute("alter table raw.yellow_trips drop constraint test_reject_second_row")

    run(*load, replacement)
    assert trip_snapshot(db) == [
        (JANUARY, 1, january.name, 15),
        (JANUARY, 2, january.name, 18),
    ]
    attempts = db.execute(
        """
        select source_filename, status, row_count, error_message
        from raw.load_attempts
        where source_kind = 'yellow_trips' and source_month = %s
        order by started_at
        """,
        (JANUARY,),
    ).fetchall()
    assert [(filename, status) for filename, status, _, _ in attempts] == [
        (january.name, "succeeded"),
        (bad_schema.name, "failed"),
        (january.name, "failed"),
        (january.name, "succeeded"),
    ]
    assert "fare_amount" in attempts[1][3]
    assert "test_reject_second_row" in attempts[2][3]
    assert attempts[-1][2] == 2


def test_download_failure_is_audited_and_retry_loads_the_month(
    postgres, write_trips, monkeypatch
):
    _, db = postgres
    source = write_trips("january.parquet", [trip()]).read_bytes()
    filename = "yellow_tripdata_2024-01.parquet"
    url = f"{ingest.TLC_ROOT}/trip-data/{filename}"
    command = ["load-trips", "--year", "2024", "--month", "1"]

    def fail(request, *, timeout):
        assert request.full_url == url
        raise HTTPError(url, 503, "Service Unavailable", {}, None)

    monkeypatch.setattr(ingest, "urlopen", fail)
    assert main(command) == 1
    assert trip_snapshot(db) == []

    def succeed(request, *, timeout):
        assert request.full_url == url
        return DownloadResponse(source, len(source))

    monkeypatch.setattr(ingest, "urlopen", succeed)
    assert main(command) == 0
    assert trip_snapshot(db) == [(JANUARY, 1, filename, 10)]
    attempts = db.execute(
        """
        select status, row_count, sha256, error_message
        from raw.load_attempts where source_filename = %s
        order by attempt_id
        """,
        (filename,),
    ).fetchall()
    assert attempts[0][:3] == ("failed", None, None)
    assert "503" in attempts[0][3]
    assert attempts[1] == ("succeeded", 1, sha256(source).hexdigest(), None)


def test_reporting_grain_quality_counts_and_fare_denominator(
    postgres, write_trips, zones_csv
):
    run, db = postgres
    january = write_trips(
        "january.parquet",
        [
            trip(),
            trip(),
            trip(fare=-5, distance=5),
            trip(pickup_zone=900),
            trip(month=2),
        ],
    )
    run("load-zones", "--file", zones_csv)
    run("load-trips", "--year", 2024, "--month", 1, "--file", january)
    run("build")

    assert db.execute(
        "select count(*), count(distinct trip_id) from analytics.fct_trips"
    ).fetchone() == (5, 5)
    assert db.execute(
        """
        select source_record_count, flagged_record_count, reportable_trip_count
        from analytics.monthly_quality_metrics where source_month = %s
        """,
        (JANUARY,),
    ).fetchone() == (5, 3, 3)
    assert db.execute(
        """
        select pickup_date, pickup_hour, trip_count, flagged_trip_count
        from analytics.hourly_demand
        """
    ).fetchall() == [(date(2024, 1, 9), 8, 3, 1)]

    query = """
        select pickup_date, pickup_zone_id, trip_count, flagged_trip_count,
               fare_trip_count, distance_trip_count, total_fare_amount,
               average_fare_amount, average_trip_distance
        from analytics.daily_zone_metrics
    """
    expected = [(date(2024, 1, 9), 1, 3, 1, 2, 3, 20, 10, 3)]
    assert db.execute(query).fetchall() == expected

    run("load-trips", "--year", 2024, "--month", 1, "--file", january)
    run("build")
    assert db.execute(query).fetchall() == expected
