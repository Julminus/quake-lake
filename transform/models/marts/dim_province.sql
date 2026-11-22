-- The 38 provinces, with the reference points silver measures distance to.
select
    p.province_code,
    p.province,
    r.region,
    count(*) as reference_points,
    round(avg(p.latitude), 4) as mean_latitude,
    round(avg(p.longitude), 4) as mean_longitude
from {{ ref('stg_reference__provinces') }} as p
left join {{ ref('province_regions') }} as r on r.province_code = p.province_code
group by p.province_code, p.province, r.region
