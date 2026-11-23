# Replicate the NYC yellow taxi pipeline

Use this playbook to reproduce the published results from this repository or rebuild the same pipeline in a new repository. Start with January 2024, verify the results and recovery behavior, then add February and March. Run the Airflow check separately so you can identify which execution path produced each result.

## Environment and inputs

Use a Docker engine running Linux containers and Docker Compose v2. Start in the folder containing `compose.yaml`. Docker handles Python and PostgreSQL for the main workflow. Host Python 3.12 is required only for the optional host test commands below.

| Component | Reference version or image |
| --- | --- |
| CLI runtime | Python 3.12, `python:3.12-slim-bookworm` |
| PostgreSQL | `postgres:17.11` |
| Ingestion | PyArrow 19.0.1 and psycopg 3.2.6 with binary wheels |
| Transformation | dbt Core 1.10.15 and dbt-postgres 1.9.1 |
| Orchestration | `apache/airflow:3.1.7-python3.12` |

The loader downloads the TLC Yellow Taxi Trip Records Parquet files and the Taxi Zone Lookup Table CSV. Use the checksums below when exact data parity matters. TLC may revise an upstream file, so record the actual checksum and the date you retrieved it. [NYC TLC source files](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

| Input | Verified rows | SHA-256 |
| --- | ---: | --- |
| `taxi_zone_lookup.csv` | 265 | `1a99e105092230f8620f301edcca7f80d3080642ff404d28ed957d3fa222c8ed` |
| `yellow_tripdata_2024-01.parquet` | 2,964,624 | `c4d59da7bbc8abaeeeb1727947ee93d9891a71acb42854bd80db1571b2030510` |
| `yellow_tripdata_2024-02.parquet` | 3,007,526 | `c76c43c18c6c6664080dd920baab4928988d5786a6b65980792ca7cd796f9f20` |
| `yellow_tripdata_2024-03.parquet` | 3,582,628 | `2d4cdc8fb96726cdd3803b13b02d2e61e71d45720aff0ebc693a8bdd1f249823` |

Copy `.env.example` to `.env` with `cp .env.example .env` in Bash or `Copy-Item .env.example .env` in PowerShell. Set a nonempty local `PGPASSWORD` and retain the defaults `PGUSER=taxi` and `PGDATABASE=taxi` for the commands below. For a fresh verification environment, set `COMPOSE_PROJECT_NAME=taxi-replication` in `.env` to create separate named volumes. If another stack uses ports 5432 or 8080, choose unused values for `POSTGRES_HOST_PORT` and `AIRFLOW_HOST_PORT`. Keep `.env` and downloaded data outside Git.

## Reproduce the warehouse

### Build and verify January

Run these from the project root in order.

```sh
docker version
docker compose version
docker compose config --quiet
docker compose build pipeline
docker compose up -d --wait postgres
docker compose run --rm pipeline load-zones
docker compose run --rm pipeline load-trips --year 2024 --month 1
docker compose run --rm pipeline build
docker compose exec postgres psql -U taxi -d taxi
```

Run the following at the `psql` prompt. Enter `\q` when finished. On a fresh database, the trip and fact counts should both be 2,964,624, the zone lookup should have 265 rows, and the build should pass all 22 tests.

```sql
select source_filename, status, row_count, sha256
from raw.load_attempts
order by attempt_id;

select
    (select count(*) from raw.taxi_zones) as zone_rows,
    (select count(*) from raw.yellow_trips) as raw_trip_rows,
    (select count(*) from analytics.fct_trips) as fact_rows;

select source_month, source_record_count, reportable_trip_count, flagged_record_count
from analytics.monthly_quality_metrics
order by source_month;
```

You can load a downloaded file from an ignored local `data/` directory. Mount it read-only so the host file stays available for a byte-identical rerun.

```sh
docker compose run --rm -v "./data:/data:ro" pipeline load-zones --file /data/taxi_zone_lookup.csv
docker compose run --rm -v "./data:/data:ro" pipeline load-trips --year 2024 --month 1 --file /data/yellow_tripdata_2024-01.parquet
```

### Verify a rejected load and a rerun

The snapshot query records raw and fact counts and fare totals, plus fingerprints of the daily, hourly and monthly quality marts. Capture the baseline after building January.

**Bash**

```bash
docker compose exec -T postgres psql -U taxi -d taxi -XAt \
  -v ON_ERROR_STOP=1 -v source_month=2024-01-01 \
  < queries/rerun_snapshot.sql > before.txt
```

**PowerShell**

```powershell
Get-Content queries/rerun_snapshot.sql |
  docker compose exec -T postgres psql -U taxi -d taxi -XAt -v ON_ERROR_STOP=1 -v source_month=2024-01-01 > before.txt
```

Run the following command as a deliberate failure. Expect a nonzero exit and a failed audit attempt for `missing.parquet`. Repeat your snapshot command with `after-failure.txt` as the output file.

```sh
docker compose run --rm pipeline load-trips --year 2024 --month 1 --file /missing.parquet
```

Reload January from the same input bytes, rebuild, and save the snapshot as `after-rerun.txt`.

```sh
docker compose run --rm pipeline load-trips --year 2024 --month 1
docker compose run --rm pipeline build
```

Both snapshots should match `before.txt`. Use `diff -u before.txt after-rerun.txt` in Bash or `Compare-Object (Get-Content before.txt) (Get-Content after-rerun.txt)` in PowerShell. A new audit row and load timestamp are expected. This exercise rejects an input before replacement. The integration suite also forces an error during COPY to test transaction rollback.

### Expand to the quarter

```sh
docker compose run --rm pipeline load-trips --year 2024 --month 2
docker compose run --rm pipeline load-trips --year 2024 --month 3
docker compose run --rm pipeline build
```

Repeat the quality query and the January snapshot. Expect the following results. The flagged and demand-eligible counts can overlap because a trip with a valid pickup can still fail a fare or distance rule.

| Source month | Source rows | Demand-eligible rows | Flagged rows |
| --- | ---: | ---: | ---: |
| 2024-01 | 2,964,624 | 2,954,246 | 56,761 |
| 2024-02 | 3,007,526 | 2,997,851 | 58,452 |
| 2024-03 | 3,582,628 | 3,571,038 | 80,324 |
| Total | 9,554,778 | 9,523,135 | 195,537 |

Run [the reporting SQL](../queries/operations.sql) for zone rankings, weekday and hourly demand, and weighted fare and distance averages. With these three months loaded, Midtown Center ranks first with 453,822 eligible pickup records. For benchmark timings and test results, see [the verification record](evidence.md).

## Reproduce the scheduler run

For the same verification conditions as the recorded Airflow run, stop the warehouse stack with `docker compose down` and set `COMPOSE_PROJECT_NAME=taxi-airflow-replication` in `.env` before starting the scheduler stack. This gives the Airflow exercise fresh volumes. Create `compose.manual.yaml` next to `compose.yaml` with the following override. It keeps the scheduler active for manual DAG runs and prevents periodic runs from adding other months during this exercise.

```yaml
services:
  airflow:
    environment:
      AIRFLOW__SCHEDULER__USE_JOB_SCHEDULE: "false"
```

```sh
docker compose -f compose.yaml -f compose.manual.yaml up -d --build airflow
docker compose -f compose.yaml -f compose.manual.yaml logs -f airflow
```

Open the Airflow UI at `http://localhost:8080` or the configured host port. Sign in as `admin` with the password generated in the standalone log. Unpause `nyc_taxi_monthly`, choose **Trigger DAG**, and enter `2020-04` for `source_month`. Verify that `load_zones`, `load_trips`, and `build_reporting` finish successfully in that order.

The verified April file produced 238,073 source rows, 235,999 demand-eligible rows, and 3,877 flagged rows. The reporting task passed all 22 dbt tests. Stop this stack with `docker compose -f compose.yaml -f compose.manual.yaml down` when finished. Named volumes persist. For regular monthly operation, start Airflow with the base configuration and the Airflow profile. [Recorded scheduler output](evidence/airflow-run.txt)

## Rebuild the project from an empty repository

Use the files linked below as implementation references. Complete each checkpoint before moving on so you can isolate errors to one layer.

| Build order | Implement | Checkpoint |
| --- | --- | --- |
| 1. Source contract | Define the supported Yellow Taxi columns, optional surcharge fields, local timestamp semantics, and zone lookup headers. Resolve names case-insensitively and reject ambiguous names. | The January file and lookup validate. A Parquet file missing a required column fails with its name. |
| 2. PostgreSQL tables | Create `raw.yellow_trips`, `raw.taxi_zones`, and `raw.load_attempts`. Key trips on `(source_month, source_row_number)`. Store the input filename and load timestamp. | A source row can be traced back to its file and ordinal. Records with identical trip attributes both load. |
| 3. Ingestion and CLI | Accept year/month and optional local path. Stage downloads, compute SHA-256, validate the schema, and stream Parquet batches to COPY. Replace the selected source month and mark its attempt successful in one transaction. | Load January twice and compare counts. Force a COPY error and confirm the preceding committed data survives. |
| 4. dbt project | Declare the raw sources, build the zone dimension and trip staging view, materialize the fact, and then aggregate daily, hourly, and monthly reporting tables. Use the shared PostgreSQL environment variables and target the `analytics` schema. | Verify grains, zone relationships, metric denominators, and raw-to-report count reconciliation. |
| 5. Compose | Add a PostgreSQL service, a Python CLI build target, and an Airflow build target. Use TCP readiness and named volumes. Give the CLI and Airflow task environment the same warehouse connection settings. | Start from a fresh volume and run the first-month commands. |
| 6. Airflow | Chain the existing CLI tasks. Set the UTC monthly schedule, historical month parameter, retry policy, and one active run. Install pipeline dependencies in a separate venv inside the Airflow image. | Run the historical April check and inspect all three task logs. |
| 7. Tests and CI | Use small fixture files for failure and recovery tests. Run dbt data tests in every reporting build. Configure CI to test against PostgreSQL, validate Compose, build both images and discover the DAG. | The 10 Python tests and 22 dbt tests pass. The real-source checks above match. |

### Keep these implementation contracts

- **Audit and transactions.** Commit the initial `running` audit entry before fetching input. Keep the month delete, COPY, and successful audit update in one transaction. Record ordinary failures separately after rollback. An interrupted process may leave a `running` attempt. A warehouse advisory lock serializes ingestion and supported dbt builds. [Database setup](../src/taxi_pipeline/database.py) · [Loader](../src/taxi_pipeline/ingest.py) · [CLI entry point](../src/taxi_pipeline/__main__.py)
- **Reporting grain.** The fact contains one row per loaded source record. The daily mart groups demand by pickup date and zone, the hourly mart by pickup date and hour, and the quality mart by source month. Keep per-measure eligibility and aggregate eligible numerators and denominators separately. [dbt project](../dbt/dbt_project.yml) · [Model definitions](../dbt/models/models.yml) · [Exact quality rules](data.md)
- **Scheduling and publication lag.** Run at midnight UTC on the first of each month with catchup disabled. A May 1 run closes the April interval and loads February. A manual `source_month` overrides this selection. Retry a failed task twice with ten minutes between attempts. [Airflow DAG](../dags/nyc_taxi_monthly.py) · [Design notes](design.md)
- **Packaging and checks.** Reuse the same Python entry point in the CLI container and the Airflow tasks. Check PostgreSQL over TCP so dependent containers wait for the main server on first initialization. Keep fixture tests and full public-data verification separate so the fixture suite can run without public-data downloads. [Dockerfile](../Dockerfile) · [Compose](../compose.yaml) · [CI workflow](../.github/workflows/verify.yml)

### Run the fixture suite

With Compose PostgreSQL healthy and Python 3.12 available, use a local venv. Set the port to your PostgreSQL host port. This example uses a local password without spaces or quotes. The database test role needs permission to create disposable databases.

```bash
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt 'pytest>=7.4,<9'
export PYTHONPATH=src
read -r -s -p 'Local PostgreSQL password: ' TAXI_TEST_PASSWORD
printf '\n'
export TEST_POSTGRES_DSN="host=127.0.0.1 port=5432 dbname=postgres user=taxi password=$TAXI_TEST_PASSWORD"
python -m pytest -q
unset TEST_POSTGRES_DSN TAXI_TEST_PASSWORD
```

Expect 10 passed with PostgreSQL configured. With no `TEST_POSTGRES_DSN`, the database cases are skipped. For PowerShell venv and environment variable syntax, see the [README](../README.md#tests-and-run-evidence).

## Record the replication

Save the source filenames and checksums, build and load runtimes, monthly result rows, snapshot comparisons, Python and dbt test summaries, and the Airflow task states. Record the versions actually used. The dependencies pin the direct Python packages, while some transitive packages and image tags can change. Capture the resolved dependency versions and image digests if another run needs to match the environment exactly.
