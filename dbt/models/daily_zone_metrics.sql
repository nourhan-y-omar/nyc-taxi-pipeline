select
    pickup_date,
    pickup_zone_id,
    count(*) as trip_count,
    count(*) filter (where has_quality_issue) as flagged_trip_count,
    count(reportable_fare_amount) as fare_trip_count,
    sum(reportable_fare_amount) as total_fare_amount,
    round(avg(reportable_fare_amount), 2) as average_fare_amount,
    count(reportable_trip_distance) as distance_trip_count,
    sum(reportable_trip_distance) as total_trip_distance,
    round(avg(reportable_trip_distance), 2) as average_trip_distance
from {{ ref('fct_trips') }}
where is_reportable_trip
group by pickup_date, pickup_zone_id
