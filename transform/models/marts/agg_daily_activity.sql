-- Indonesian earthquakes per UTC day, with a row for every day from the first to the
-- last, so a quiet day is a 0 rather than a gap.
with earthquakes as (
    select * from {{ ref('fct_earthquakes') }} where in_indonesia
),

days as (
    select unnest(generate_series(first_day, last_day, interval 1 day))::date as activity_date
    from (
        select min(origin_date_utc) as first_day, max(origin_date_utc) as last_day
        from earthquakes
    )
)

select
    d.activity_date,
    -- count(*) filter, not count_if: DuckDB's count_if returns a 128-bit integer.
    count(e.event_id) as earthquakes,
    count(e.event_id) filter (where e.magnitude >= 5) as m5_plus,
    count(e.event_id) filter (where e.magnitude >= 6) as m6_plus,
    count(e.event_id) filter (where e.depth_class = 'shallow') as shallow,
    max(e.magnitude) as max_magnitude
from days as d
left join earthquakes as e on e.origin_date_utc = d.activity_date
group by d.activity_date
