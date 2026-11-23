select 'Zone lookup has no records' as error
where not exists (select 1 from {{ ref('dim_zones') }})
