# Verification record

Executed on 11 November 2026 with Python 3.12.3, PostgreSQL 17.11, dbt Core 1.10.15 and dbt-postgres 1.9.1. The verification used the NYC TLC lookup and the published January through March 2024 yellow taxi files.

## Load and recovery

| Attempt | File | Status | Committed rows | Elapsed seconds |
| --- | --- | --- | ---: | ---: |
| 1 | `taxi_zone_lookup.csv` | succeeded | 265 | 0.481 |
| 2 | `yellow_tripdata_2024-01.parquet` | succeeded | 2,964,624 | 248.158 |
| 3 | `missing-columns.parquet` | failed | — | 0.221 |
| 4 | `yellow_tripdata_2024-01.parquet` | succeeded | 2,964,624 | 226.267 |
| 5 | `yellow_tripdata_2024-02.parquet` | succeeded | 3,007,526 | 210.931 |
| 6 | `yellow_tripdata_2024-03.parquet` | succeeded | 3,582,628 | 148.659 |

The CLI downloaded the lookup and first January file directly. The rerun, February, and March used local copies downloaded from TLC. The controlled failure submitted a January Parquet file missing required columns. The command exited with status 1, logged the missing column names, and recorded a failed attempt. January's raw and fact counts and metered-fare totals matched the original snapshot, along with fingerprints of its daily, hourly, and quality marts. These checks matched again after reloading the original January file and adding February and March.

Reproduce the comparison with [the snapshot query](../queries/rerun_snapshot.sql). After the first successful build, run `psql -X -At -v source_month=2024-01-01 -f queries/rerun_snapshot.sql` and save the result. Reload the same January input, run `build`, and execute the snapshot query again. The sorted output should match.

## Reporting results

The final warehouse contained **9,554,778** trip records across three source months. **9,523,135** qualified for pickup demand, and **195,537** had at least one quality flag.

| Source month | Source records | Demand-eligible | Flagged records | Flagged % | Avg. fare | Avg. miles |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| 2024-01 | 2,964,624 | 2,954,246 | 56,761 | 1.91% | $18.64 | 3.23 |
| 2024-02 | 3,007,526 | 2,997,851 | 58,452 | 1.94% | $18.54 | 3.15 |
| 2024-03 | 3,582,628 | 3,571,038 | 80,324 | 2.24% | $19.26 | 3.28 |

The independent Arrow analysis matched 12 measures per month across the two reporting grains and the quality mart, including row counts, eligible denominators, total metered fare, and total distance. The [reporting SQL](../queries/operations.sql) also covers pickup-zone rankings and demand by weekday and hour.

## Test results

- Python and PostgreSQL integration suite returned `10 passed in 118.89s (0:01:58)`. Scenarios included an HTTP 503 followed by a successful retry, database COPY failure after replacement began, a same-month rerun, concurrent first loads, a corrected month, and reporting denominators.
- dbt passed all 22 data tests and built six models in both runs. dbt reported 90.46 seconds for January and 494.11 seconds for the quarter.
- Airflow 3.1.7 imported the DAG and selected the expected source month for a manual January run and the May 1 schedule in a task-level smoke check.

## Docker and Airflow verification

Both images built and the Compose stack started in an isolated Alpine Linux guest with Docker Engine 28.3.3 and Compose v2.36.2. A fresh PostgreSQL volume passed the TCP health check and answered a SQL query immediately afterward. This verified the fix for the initial socket check, which had passed while PostgreSQL was still initializing the volume.

The Airflow scheduler completed a manually triggered run using the full April 2020 TLC file. That file made the emulated test shorter. Periodic launches were disabled in the test guest so the scheduler could focus on the selected historical month. All three tasks succeeded on their first attempt, and the DAG completed in 554.72 seconds in the guest. dbt built all six models and passed all 22 data tests.

PostgreSQL held 238,073 April trip records, with 235,999 eligible for pickup demand and 3,877 flagged. The Airflow results matched an independent calculation from the Parquet file for the daily and hourly row counts, fare and distance denominators, and fare and distance totals. An earlier January run in this guest was stopped during COPY and left zero committed January records. The interrupted load attempt kept its `running` audit entry, as described in the design notes.

See the [scheduler and warehouse output](evidence/airflow-run.txt).

## Screenshots

- [Load audit and test output](evidence/pipeline-run.png)
- [Quarterly reporting results and top pickup zones](evidence/reporting-results.png)

The screenshots show the database query results and captured verification output.
