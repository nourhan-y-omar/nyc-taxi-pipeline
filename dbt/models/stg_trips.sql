with mapped as (
    select
        trips.*,
        pickup.location_id as pickup_zone_id,
        dropoff.location_id as dropoff_zone_id,
        to_char(trips.source_month, 'YYYY-MM') || ':' || trips.source_row_number::text as trip_id
    from {{ source('raw', 'yellow_trips') }} as trips
    left join {{ ref('dim_zones') }} as pickup on pickup.location_id = trips.pickup_location_id
    left join {{ ref('dim_zones') }} as dropoff on dropoff.location_id = trips.dropoff_location_id
), checked as (
    select
        *,
        pickup_at is null
            or pickup_at < source_month
            or pickup_at >= source_month + interval '1 month' as invalid_pickup_timestamp,
        dropoff_at is null
            or pickup_at is null
            or dropoff_at <= pickup_at
            or dropoff_at > pickup_at + interval '24 hours' as invalid_dropoff_timestamp,
        pickup_zone_id is null or pickup_zone_id = 264 as invalid_pickup_zone,
        dropoff_zone_id is null or dropoff_zone_id = 264 as invalid_dropoff_zone,
        not coalesce(trip_distance between 0 and 200, false) as invalid_trip_distance,
        not coalesce(fare_amount between 0 and 1000, false) as invalid_fare_amount,
        not coalesce(total_amount >= 0 and total_amount < 'Infinity'::float8, false) as invalid_total_amount
    from mapped
)
select
    *,
    invalid_pickup_timestamp or invalid_dropoff_timestamp
        or invalid_pickup_zone or invalid_dropoff_zone
        or invalid_trip_distance or invalid_fare_amount or invalid_total_amount as has_quality_issue,
    not (invalid_pickup_timestamp or invalid_pickup_zone) as is_reportable_trip,
    case when not invalid_pickup_timestamp then pickup_at::date end as pickup_date,
    case when not invalid_pickup_timestamp then extract(hour from pickup_at)::smallint end as pickup_hour,
    case when not invalid_fare_amount then fare_amount::numeric end as reportable_fare_amount,
    case when not invalid_trip_distance then trip_distance::numeric end as reportable_trip_distance
from checked
