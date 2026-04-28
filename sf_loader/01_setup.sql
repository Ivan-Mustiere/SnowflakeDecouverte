-- ============================================================
-- Setup initial Snowflake pour le projet NYC Taxi
-- À exécuter UNE FOIS depuis l'UI Snowflake (rôle ACCOUNTADMIN).
-- ============================================================

-- 1. Warehouse de calcul
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH
    WAREHOUSE_SIZE = 'X-SMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;

-- 2. Base et schémas (pattern medallion)
CREATE DATABASE IF NOT EXISTS NYC_TAXI;

USE DATABASE NYC_TAXI;

CREATE SCHEMA IF NOT EXISTS RAW;       -- données chargées telles quelles
CREATE SCHEMA IF NOT EXISTS STAGING;   -- transformations DBT (vues)
CREATE SCHEMA IF NOT EXISTS MARTS;     -- modèle dimensionnel final

-- 3. File format Parquet
CREATE OR REPLACE FILE FORMAT NYC_TAXI.RAW.PARQUET_FORMAT
    TYPE = PARQUET;

-- 4. Stage externe pointant vers MinIO
--    Note : Snowflake ne se connecte pas directement à un MinIO local depuis le cloud.
--    Pour le projet, on utilise donc un STAGE INTERNE et on push les Parquet
--    via PUT depuis Airflow (snowflake-connector-python).
CREATE OR REPLACE STAGE NYC_TAXI.RAW.LAKE_STAGE
    FILE_FORMAT = NYC_TAXI.RAW.PARQUET_FORMAT
    COMMENT = 'Stage interne alimenté par Airflow (PUT) depuis MinIO';

-- 5. Table cible RAW
CREATE OR REPLACE TABLE NYC_TAXI.RAW.YELLOW_TRIPS (
    vendorid                INTEGER,
    tpep_pickup_datetime    TIMESTAMP_NTZ,
    tpep_dropoff_datetime   TIMESTAMP_NTZ,
    passenger_count         INTEGER,
    trip_distance           FLOAT,
    ratecodeid              INTEGER,
    store_and_fwd_flag      STRING,
    pulocationid            INTEGER,
    dolocationid            INTEGER,
    payment_type            INTEGER,
    fare_amount             FLOAT,
    extra                   FLOAT,
    mta_tax                 FLOAT,
    tip_amount              FLOAT,
    tolls_amount            FLOAT,
    improvement_surcharge   FLOAT,
    total_amount            FLOAT,
    congestion_surcharge    FLOAT,
    -- colonnes ajoutées par Spark
    trip_duration_min       FLOAT,
    avg_speed_mph           FLOAT,
    price_per_mile          FLOAT,
    tip_pct                 FLOAT,
    pickup_date             DATE,
    pickup_year             INTEGER,
    pickup_month            INTEGER,
    pickup_day              INTEGER,
    pickup_hour             INTEGER,
    pickup_dayofweek        INTEGER,
    is_weekend              BOOLEAN,
    -- métadonnées de chargement
    _loaded_at              TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- 6. Table de monitoring (bonus)
CREATE TABLE IF NOT EXISTS NYC_TAXI.RAW.PIPELINE_RUNS (
    run_id          STRING,
    execution_date  DATE,
    step            STRING,
    status          STRING,
    rows_processed  INTEGER,
    duration_sec    FLOAT,
    error_message   STRING,
    created_at      TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- 7. Vérifications
SHOW WAREHOUSES LIKE 'COMPUTE_WH';
SHOW SCHEMAS IN DATABASE NYC_TAXI;
SHOW TABLES IN SCHEMA NYC_TAXI.RAW;
