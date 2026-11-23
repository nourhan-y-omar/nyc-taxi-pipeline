# Data dictionary

All trip counts refer to source records. Two rows with the same timestamps, fare, and locations can both contribute to a count. Trip dates and hours use New York local wall-clock time.

## Table grains

| Relation | One row represents |
| --- | --- |
| `raw.yellow_trips` | One physical record in the currently loaded version of a monthly Parquet file. The key is `(source_month, source_row_number)`. |
| `raw.taxi_zones` | One location ID in the currently loaded TLC lookup. |
| `raw.load_attempts` | One attempt to load a trip file or the zone lookup. |
| `analytics.stg_trips` | One raw trip record with standardized values and quality flags. |
| `analytics.dim_zones` | One TLC location ID with borough, zone name, and service zone. |
| `analytics.fct_trips` | One source trip record, including records that fail a quality rule. |
| `analytics.daily_zone_metrics` | One local pickup date and eligible pickup location. |
| `analytics.hourly_demand` | One local pickup date and clock hour with eligible pickups. |
| `analytics.monthly_quality_metrics` | One source month with loaded trip records. |

## Source and load columns

| Column or group | Meaning |
| --- | --- |
| `source_month`, `source_row_number` | First day of the file's named month and one-based physical row position. |
| `source_filename`, `loaded_at` | Loaded input basename and UTC load timestamp. |
| `pickup_at`, `dropoff_at` | Meter engagement and disengagement, stored as local timestamps. |
| `pickup_location_id`, `dropoff_location_id` | TLC zone codes for those events. |
| `vendor_id`, `rate_code_id`, `payment_type` | TLC codes for the submitter, final rate, and payment category. |
| `passenger_count`, `store_and_fwd_flag` | Reported passengers and whether the vehicle held the record for later transmission. |
| `trip_distance` | Taximeter distance in miles. |
| `fare_amount`, `total_amount` | Metered time-and-distance fare and total amount charged, in USD. The total excludes cash tips. |
| `extra`, `mta_tax`, `tip_amount`, `tolls_amount` | Source-reported charge components in USD. The recorded tips omit cash tips. |
| `improvement_surcharge`, `congestion_surcharge`, `airport_fee` | TLC surcharge fields in USD. |
| `additional_fields` | JSON representation of extra source columns outside the mapped fields. |
| `location_id`, `borough`, `zone`, `service_zone` | TLC zone lookup attributes. |
| `attempt_id`, `source_kind`, `status` | Unique load attempt, input kind, and run state. |
| `row_count`, `sha256`, `error_message` | Committed input rows, input checksum when available, and failure details. |
| `started_at`, `finished_at`, `duration_seconds` | Audit timestamps and elapsed load time. |

See the [TLC source dictionary](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf) for published code definitions.

## Quality rules

| Area | Flagged when |
| --- | --- |
| Pickup timestamp | Missing or outside the file's source month. |
| Trip duration | Pickup or drop-off is missing, or duration is outside the interval of greater than zero and at most 24 hours. |
| Pickup or drop-off zone | Missing, absent from the lookup, or equal to TLC's Unknown location 264. Location 265 is the published Outside of NYC category. |
| Distance | Missing, non-finite, negative, or above 200 miles. |
| Metered fare | Missing, non-finite, negative, or above USD 1,000. |
| Total charge | Missing, non-finite, or negative. |

Distance and fare limits are reporting plausibility rules. Quality rates count any flagged row once per source month. An analyst can inspect individual flags in the trip fact, and a row can contribute to several reason counts.

Demand requires an in-month pickup and a usable pickup zone. Fare and distance each add their own eligibility rule to that demand population. Use summed numerators divided by summed eligible-record counts to roll up averages across dates or zones. A group with no eligible measurements has a null average.

| Reporting column | Meaning |
| --- | --- |
| `trip_id` | Source month and row ordinal rendered as `YYYY-MM:row`. |
| `pickup_zone_id`, `dropoff_zone_id` | Resolved lookup IDs. Unmapped raw codes have null lookup IDs. |
| `pickup_date`, `pickup_hour` | New York calendar date and hour, present when the pickup timestamp belongs to the source month. |
| `invalid_*`, `has_quality_issue` | Individual rule outcomes and their logical OR. |
| `is_reportable_trip` | Whether the row qualifies for pickup demand. |
| `reportable_fare_amount`, `reportable_trip_distance` | Numeric metric values when the corresponding value passes its own rule, otherwise null. Aggregates also apply the demand population filter. |
| `trip_count`, `flagged_trip_count` | Eligible pickups and the subset with one or more quality flags in the daily and hourly marts. |
| `fare_trip_count`, `distance_trip_count` | Eligible records for each daily metric. |
| `total_fare_amount`, `total_trip_distance` | Sums of eligible metered fares in USD and distances in miles. |
| `average_fare_amount`, `average_trip_distance` | Daily zone metric averages rounded to two decimal places. |
| `source_record_count`, `flagged_record_count`, `reportable_trip_count` | All source records, records flagged by at least one rule, and records eligible for demand. |
| `invalid_*_count` | Source records flagged by the corresponding rule. These counts can overlap. |
