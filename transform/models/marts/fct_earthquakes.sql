-- One row per earthquake in the USGS box, Indonesian or not (in_indonesia says which).
--
-- Incremental: silver is rebuilt in full each run, so there is no "new rows since"
-- column to filter on. Each run compares a hash of every row with the hashes already
-- here, rewrites only the events that are new or changed, and deletes events silver no
-- longer has (a BMKG event later merged into USGS, an id USGS replaced).
{{
    config(
        materialized='incremental',
        unique_key='event_id',
        incremental_strategy='delete+insert',
        on_schema_change='fail',
        post_hook="delete from {{ this }} where event_id not in (select event_id from {{ ref('int_earthquakes') }})",
    )
}}

with earthquakes as (
    select e.*, md5(to_json(e)) as row_hash
    from {{ ref('int_earthquakes') }} as e
)

select * from earthquakes
{% if is_incremental() %}
where row_hash not in (select row_hash from {{ this }})
{% endif %}
