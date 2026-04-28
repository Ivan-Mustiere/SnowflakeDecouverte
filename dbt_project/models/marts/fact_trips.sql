{{ config(
    materialized='table',
    cluster_by=['pickup_date_key']
) }}

-- Table de faits : 1 ligne = 1 course.
-- Granularité : course individuelle.
-- Clés étrangères vers les 4 dimensions : date, vendor, payment, zone (x2).

with trips as (
    select * from {{ ref('stg_trips') }}
)

select
    -- Clé technique
    {{ dbt_utils.generate_surrogate_key([
        'vendor_id', 'pickup_at', 'pickup_location_id', 'dropoff_location_id'
    ]) }}                                       as trip_key,

    -- Foreign keys vers les dimensions
    pickup_date                                 as pickup_date_key,
    vendor_id                                   as vendor_id,
    payment_type_id                             as payment_type_id,
    pickup_location_id                          as pickup_zone_id,
    dropoff_location_id                         as dropoff_zone_id,

    -- Attributs de course
    pickup_at,
    dropoff_at,
    pickup_hour,
    is_weekend,
    passenger_count,
    rate_code_id,

    -- Mesures (faits additifs)
    trip_distance_miles,
    trip_duration_min,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    congestion_surcharge,
    total_amount,

    -- Mesures dérivées
    avg_speed_mph,
    price_per_mile,
    tip_pct

from trips
