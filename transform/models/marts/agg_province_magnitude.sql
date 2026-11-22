-- Indonesian earthquakes per province (nearest reference point) and magnitude class.
select
    province_code,
    magnitude_class,
    count(*) as earthquakes,
    max(magnitude) as max_magnitude,
    round(avg(depth_km), 1) as mean_depth_km,
    min(origin_time_utc) as first_utc,
    max(origin_time_utc) as last_utc
from {{ ref('fct_earthquakes') }}
where in_indonesia
group by province_code, magnitude_class
