-- One row per reference point (regency, city or lake) with its province.
select
    province_code,
    province,
    regency,
    latitude::double as latitude,
    longitude::double as longitude
from {{ source('reference', 'provinces') }}
