-- Both aggregates must count each Indonesian earthquake in fct_earthquakes exactly once.
with totals as (
    select
        (select count(*) from {{ ref('fct_earthquakes') }} where in_indonesia) as fct,
        (select coalesce(sum(earthquakes), 0) from {{ ref('agg_daily_activity') }}) as daily,
        (select coalesce(sum(earthquakes), 0) from {{ ref('agg_province_magnitude') }}) as province
)

select * from totals where daily != fct or province != fct
