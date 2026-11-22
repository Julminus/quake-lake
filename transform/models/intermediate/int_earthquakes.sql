-- Earthquakes only (silver keeps other event types), classified, with the country USGS
-- names for each one.
with earthquakes as (
    select
        *,
        -- USGS's place is "<km> <direction> of <town>, <country>" or a region ("Banda Sea").
        case
            when row_source = 'usgs' then trim(string_split(place, ', ')[-1])
        end as place_region
    from {{ ref('stg_silver__events') }}
    where event_type = 'earthquake'
)

select
    e.event_id,
    e.row_source,
    e.origin_time_utc,
    e.origin_time_utc::date as origin_date_utc,
    e.updated_utc,
    e.latitude,
    e.longitude,
    e.depth_km,
    -- USGS's depth classes.
    case
        when e.depth_km < 70 then 'shallow'
        when e.depth_km < 300 then 'intermediate'
        else 'deep'
    end as depth_class,
    e.magnitude,
    e.mag_type,
    -- USGS's magnitude classes; it has none below minor (3 to 3.9).
    case
        when e.magnitude is null then 'unknown'
        when e.magnitude < 3 then 'micro'
        when e.magnitude < 4 then 'minor'
        when e.magnitude < 5 then 'light'
        when e.magnitude < 6 then 'moderate'
        when e.magnitude < 7 then 'strong'
        when e.magnitude < 8 then 'major'
        else 'great'
    end as magnitude_class,
    e.place,
    e.place_region,
    r.country_code as place_country_code,
    -- Indonesian unless USGS's place is in another country. BMKG-only events have no
    -- USGS place and count as Indonesian.
    coalesce(r.country_code, 'ID') = 'ID' as in_indonesia,
    e.province_code,
    e.province_km,
    e.bmkg_id,
    e.bmkg_id is not null as bmkg_listed
from earthquakes as e
left join {{ ref('usgs_place_regions') }} as r on r.place_region = e.place_region
