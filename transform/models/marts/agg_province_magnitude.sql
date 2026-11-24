-- Indonesian earthquakes per province (nearest reference point) and magnitude class.
select
    province_code,
    magnitude_class,
    count(*) as earthquakes,
    max(magnitude) as max_magnitude,
    -- A float sum depends on the fact's row order, which differs between an incremental
    -- build and a full one, and flipped a mean of 27.35 when rounded; a decimal sum is
    -- exact. avg() still returns a double (100.05 is 100.0499...), so the mean goes back
    -- to 10 decimals before rounding half away from zero: exact for groups of under 20
    -- million. Depths are taken to the metre (3 decimals, all USGS sends).
    round(avg(depth_km::decimal(18, 3))::decimal(38, 10), 1)::double as mean_depth_km,
    min(origin_time_utc) as first_utc,
    max(origin_time_utc) as last_utc
from {{ ref('fct_earthquakes') }}
where in_indonesia
group by province_code, magnitude_class
