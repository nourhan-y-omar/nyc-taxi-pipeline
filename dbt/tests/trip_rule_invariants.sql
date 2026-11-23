select trip_id
from {{ ref('fct_trips') }}
where (is_reportable_trip and (pickup_date is null or pickup_hour is null or pickup_zone_id is null or pickup_zone_id = 264))
    or (invalid_fare_amount <> (reportable_fare_amount is null))
    or (invalid_trip_distance <> (reportable_trip_distance is null))
    or reportable_fare_amount not between 0 and 1000
    or reportable_trip_distance not between 0 and 200
    or (has_quality_issue is distinct from (
        invalid_pickup_timestamp or invalid_dropoff_timestamp
        or invalid_pickup_zone or invalid_dropoff_zone
        or invalid_trip_distance or invalid_fare_amount or invalid_total_amount
    ))
