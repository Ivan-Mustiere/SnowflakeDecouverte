{{ config(materialized='view') }}

-- Couche staging : nommage cohérent + filtres qualité minimaux.
-- Toutes les transformations métier sont faites en aval (marts).

with source as (
    select * from {{ source('raw', 'yellow_trips') }}
),

renamed as (
    select
        -- Identifiants
        vendorid                                            as vendor_id,
        pulocationid                                        as pickup_location_id,
        dolocationid                                        as dropoff_location_id,
        payment_type                                        as payment_type_id,
        ratecodeid                                          as rate_code_id,

        -- Timestamps & dimensions temporelles
        tpep_pickup_datetime                                as pickup_at,
        tpep_dropoff_datetime                               as dropoff_at,
        pickup_date,
        pickup_year,
        pickup_month,
        pickup_day,
        pickup_hour,
        pickup_dayofweek,
        is_weekend,

        -- Mesures
        passenger_count,
        trip_distance                                       as trip_distance_miles,
        trip_duration_min,
        avg_speed_mph,

        -- Montants
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        congestion_surcharge,
        total_amount,
        price_per_mile,
        tip_pct,

        -- Métadonnées
        store_and_fwd_flag,
        _loaded_at
    from source
)

select * from renamed
