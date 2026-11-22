-- Silver's events, renamed for gold. Times become plain UTC timestamps, so a day is a
-- UTC day whatever the session's time zone.
select
    event_id,
    source as row_source,
    timezone('UTC', "time") as origin_time_utc,
    timezone('UTC', updated) as updated_utc,
    latitude,
    longitude,
    depth_km,
    magnitude,
    mag_type,
    place,
    event_type,
    bmkg_id,
    province_code,
    province_km
from {{ source('silver', 'events') }}
