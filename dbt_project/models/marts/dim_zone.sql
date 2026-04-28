{{ config(materialized='table') }}

-- Dim Zone : enrichi depuis le seed taxi_zones.csv (265 zones officielles NYC TLC).

with zones as (
    select * from {{ source('raw', 'taxi_zones') }}
)

select
    location_id     as zone_id,
    borough,
    zone            as zone_name,
    service_zone
from zones
