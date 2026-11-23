-- Top pickup zones across the loaded period.
select
    z.location_id,
    z.borough,
    z.zone,
    sum(m.trip_count) as trips
from analytics.daily_zone_metrics as m
join analytics.dim_zones as z on z.location_id = m.pickup_zone_id
group by z.location_id, z.borough, z.zone
order by trips desc, z.location_id
limit 10;

-- Demand across local weekdays and clock hours.
select
    extract(isodow from pickup_date)::integer as weekday_monday_1,
    pickup_hour,
    count(*) as observed_dates,
    sum(trip_count) as trips,
    round(avg(trip_count), 2) as avg_trips_per_observed_date
from analytics.hourly_demand
group by 1, 2
order by 1, 2;

-- Month-level averages weighted by the number of eligible records.
select
    date_trunc('month', pickup_date)::date as pickup_month,
    sum(trip_count) as trips,
    sum(fare_trip_count) as fare_records,
    round(sum(total_fare_amount) / nullif(sum(fare_trip_count), 0), 2) as avg_metered_fare_usd,
    sum(distance_trip_count) as distance_records,
    round(sum(total_trip_distance) / nullif(sum(distance_trip_count), 0), 2) as avg_distance_mi
from analytics.daily_zone_metrics
group by 1
order by 1;

-- Quality issues over all records in each published file.
select
    source_month,
    source_record_count,
    flagged_record_count,
    round(100.0 * flagged_record_count / nullif(source_record_count, 0), 2) as flagged_pct,
    reportable_trip_count
from analytics.monthly_quality_metrics
order by source_month;

-- Inspect example flagged rows in their source context.
select
    trip_id,
    source_filename,
    pickup_at,
    dropoff_at,
    pickup_location_id,
    dropoff_location_id,
    fare_amount,
    trip_distance,
    invalid_pickup_timestamp,
    invalid_dropoff_timestamp,
    invalid_pickup_zone,
    invalid_dropoff_zone,
    invalid_trip_distance,
    invalid_fare_amount,
    invalid_total_amount
from analytics.fct_trips
where has_quality_issue
order by source_month, source_row_number
limit 50;
