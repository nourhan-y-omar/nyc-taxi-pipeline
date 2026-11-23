with source_counts as (
    select source_month, count(*) as record_count
    from {{ source('raw', 'yellow_trips') }}
    group by source_month
), daily_counts as (
    select date_trunc('month', pickup_date)::date as source_month, sum(trip_count) as trip_count
    from {{ ref('daily_zone_metrics') }}
    group by 1
), hourly_counts as (
    select date_trunc('month', pickup_date)::date as source_month, sum(trip_count) as trip_count
    from {{ ref('hourly_demand') }}
    group by 1
), quality as (
    select * from {{ ref('monthly_quality_metrics') }}
)
select source_month
from source_counts
full join quality using (source_month)
full join daily_counts using (source_month)
full join hourly_counts using (source_month)
where source_counts.record_count is distinct from quality.source_record_count
    or quality.reportable_trip_count is distinct from coalesce(daily_counts.trip_count, 0)
    or quality.reportable_trip_count is distinct from coalesce(hourly_counts.trip_count, 0)
    or quality.flagged_record_count not between 0 and quality.source_record_count
