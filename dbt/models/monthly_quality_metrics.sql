select
    source_month,
    count(*) as source_record_count,
    count(*) filter (where has_quality_issue) as flagged_record_count,
    count(*) filter (where is_reportable_trip) as reportable_trip_count,
    count(*) filter (where invalid_pickup_timestamp) as invalid_pickup_timestamp_count,
    count(*) filter (where invalid_dropoff_timestamp) as invalid_dropoff_timestamp_count,
    count(*) filter (where invalid_pickup_zone) as invalid_pickup_zone_count,
    count(*) filter (where invalid_dropoff_zone) as invalid_dropoff_zone_count,
    count(*) filter (where invalid_trip_distance) as invalid_trip_distance_count,
    count(*) filter (where invalid_fare_amount) as invalid_fare_amount_count,
    count(*) filter (where invalid_total_amount) as invalid_total_amount_count
from {{ ref('fct_trips') }}
group by source_month
