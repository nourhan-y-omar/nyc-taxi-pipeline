select
    pickup_date,
    pickup_hour,
    count(*) as trip_count,
    count(*) filter (where has_quality_issue) as flagged_trip_count
from {{ ref('fct_trips') }}
where is_reportable_trip
group by pickup_date, pickup_hour
