-- Run with psql -X -At -v source_month=2024-01-01 -f queries/rerun_snapshot.sql.
-- Compare the outputs before and after a reload of the same source bytes.
with fingerprint as (
    select 'raw' as relation, count(*)::text as records,
        coalesce(sum(fare_amount::numeric), 0)::text as total_or_checksum
    from raw.yellow_trips
    where source_month = :'source_month'::date

    union all

    select 'facts', count(*)::text, coalesce(sum(fare_amount::numeric), 0)::text
    from analytics.fct_trips
    where source_month = :'source_month'::date

    union all

    select 'daily', coalesce(sum(trip_count), 0)::text,
        md5(string_agg(m::text, E'\n' order by pickup_date, pickup_zone_id))
    from analytics.daily_zone_metrics as m
    where pickup_date >= :'source_month'::date
        and pickup_date < :'source_month'::date + interval '1 month'

    union all

    select 'hourly', coalesce(sum(trip_count), 0)::text,
        md5(string_agg(h::text, E'\n' order by pickup_date, pickup_hour))
    from analytics.hourly_demand as h
    where pickup_date >= :'source_month'::date
        and pickup_date < :'source_month'::date + interval '1 month'

    union all

    select 'quality', source_record_count::text, md5(q::text)
    from analytics.monthly_quality_metrics as q
    where source_month = :'source_month'::date
)
select relation, records, total_or_checksum
from fingerprint
order by relation;
