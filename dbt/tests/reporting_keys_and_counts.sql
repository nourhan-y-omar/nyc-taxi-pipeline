select 'daily_zone_metrics' as model, pickup_date, pickup_zone_id as bucket
from {{ ref('daily_zone_metrics') }}
group by pickup_date, pickup_zone_id
having count(*) <> 1
    or min(trip_count) <= 0
    or min(flagged_trip_count) < 0 or max(flagged_trip_count) > min(trip_count)
    or min(fare_trip_count) < 0 or max(fare_trip_count) > min(trip_count)
    or min(distance_trip_count) < 0 or max(distance_trip_count) > min(trip_count)
    or bool_or((average_fare_amount is null) <> (fare_trip_count = 0))
    or bool_or((average_trip_distance is null) <> (distance_trip_count = 0))

union all

select 'hourly_demand', pickup_date, pickup_hour
from {{ ref('hourly_demand') }}
group by pickup_date, pickup_hour
having count(*) <> 1
    or pickup_hour not between 0 and 23
    or min(trip_count) <= 0
    or min(flagged_trip_count) < 0 or max(flagged_trip_count) > min(trip_count)
