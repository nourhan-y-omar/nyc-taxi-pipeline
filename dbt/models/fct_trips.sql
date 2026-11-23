{{ config(indexes=[{'columns': ['source_month']}, {'columns': ['trip_id'], 'unique': true}]) }}

select * from {{ ref('stg_trips') }}
