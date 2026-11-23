# Design decisions

## Source month and record identity

The loader addresses TLC files by the year and month in the filename. A file can contain a pickup timestamp outside that month. Those rows belong to the file for reload and audit purposes, and the data-quality rules keep them visible for investigation.

The raw identity combines the source month with the physical row ordinal. A rerun of the same file preserves those identities and retains rows with identical business values. A corrected file can change row order, so the identity is scoped to the currently loaded version of that file. It represents a source record and carries no cross-version claim about an individual taxi ride.

## Monthly replacement

A full month is a manageable transaction. The loader uses PostgreSQL COPY and replaces only the requested source month. A failure rolls back the replacement and keeps the preceding committed data queryable. A successful replacement can remove records that the upstream publisher removed and retain records that share the same business values.

The warehouse advisory lock serializes raw mutations and supported dbt builds. Use the pipeline CLI for dbt builds so that it participates in this lock. A raw load commits before dbt runs. dbt builds tables in model-level transactions, so a failed model or data test makes the build fail and can leave a mix of prior and current tables. After correcting the cause, rerun the build to bring the marts back to a consistent release.

The audit table records each attempt with its result and checksum when available. A terminated process or a database outage during audit finalization can leave a `running` row. Check the process and timestamps before retrying. PostgreSQL rolls back an interrupted replacement, and a retry records a new attempt. If two inputs target the same month, the last successful replacement to commit determines the current version.

## Monthly orchestration

Compose waits for PostgreSQL to accept TCP connections. During first-volume initialization, the PostgreSQL image starts a temporary server on a Unix socket. A socket health check can report success before that server shuts down and the main server starts.

Airflow runs the zone load, trip load, and dbt build in sequence. The DAG schedules at midnight UTC on the first day of each month. A May 1 run closes the April interval and requests February data, allowing two full intervening calendar months for TLC publication. The schedule uses a fixed lag because file publication can vary. The DAG allows one active run, makes two retries per task with ten minutes between attempts, and disables automatic catchup. Trigger a historical month explicitly with the `source_month` parameter. A scheduled run can coincide with a manual historical run, and Airflow queues them.

When TLC has yet to publish a file, inspect the trip task log and `raw.load_attempts`. Once the file becomes available, clear the failed task to rerun it or trigger that source month explicitly. If the raw load completed and only dbt failed, correct the cause and rerun the reporting task.

## Quality and reporting

Source records remain available in raw and the trip fact. Reporting eligibility is defined for each measure. A row can be usable for pickup demand and still have a fare or distance issue. Monthly quality reporting counts source records with one or more flags, while per-rule counts can overlap. See the [data dictionary](data.md) for the exact predicates.

The fare metric is the TLC metered `fare_amount`. The source `total_amount` includes other components of the passenger charge and excludes cash tips. Distances are miles and pickup times use New York wall-clock time as supplied by TLC. The analysis makes no conversion across the repeated hour when daylight saving time ends. [TLC yellow trip data dictionary](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf)

## Next steps

Retain immutable file versions in object storage to support historical comparisons between publisher revisions. At higher volume, use PostgreSQL partition swaps or a warehouse with partition-level replacement and publish marts behind a tested release boundary. Add monitoring for shifts in source volume, freshness and quality rates once historical baselines are available.
