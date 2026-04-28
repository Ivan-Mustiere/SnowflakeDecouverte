"""
Chargement MinIO (staging Parquet) -> Snowflake RAW.YELLOW_TRIPS

Approche : on télécharge le Parquet depuis MinIO en local (tmp), on le PUT
dans le stage interne Snowflake, puis on COPY INTO la table.

Cette approche évite d'avoir à exposer MinIO publiquement (Snowflake cloud
ne peut pas lire un MinIO sur localhost).
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path

import boto3
from botocore.client import Config
from loguru import logger
import snowflake.connector


# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "nyc-taxi-lake")

SF_CONFIG = {
    "account":   os.environ["SNOWFLAKE_ACCOUNT"],
    "user":      os.environ["SNOWFLAKE_USER"],
    "password":  os.environ["SNOWFLAKE_PASSWORD"],
    "role":      os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
    "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
    "database":  os.getenv("SNOWFLAKE_DATABASE", "NYC_TAXI"),
    "schema":    "RAW",
}


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def get_s3():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def list_staging_files(prefix: str) -> list[str]:
    s3 = get_s3()
    resp = s3.list_objects_v2(Bucket=MINIO_BUCKET, Prefix=prefix)
    return [o["Key"] for o in resp.get("Contents", []) if o["Key"].endswith(".parquet")]


def download_to_tmp(key: str) -> Path:
    s3 = get_s3()
    tmp_path = Path(tempfile.gettempdir()) / Path(key).name
    s3.download_file(MINIO_BUCKET, key, str(tmp_path))
    logger.info(f"Téléchargé : {key} -> {tmp_path}")
    return tmp_path


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
def load_to_snowflake(execution_date: str) -> int:
    """
    Args:
        execution_date: 'YYYY-MM-DD'

    Returns:
        Nombre de lignes chargées.
    """
    year, month, day = execution_date.split("-")
    prefix = f"staging/yellow_taxi/year={year}/month={month}/day={day}/"

    files = list_staging_files(prefix)
    if not files:
        logger.warning(f"Aucun fichier dans {prefix}")
        return 0

    conn = snowflake.connector.connect(**SF_CONFIG)
    cur = conn.cursor()
    total_rows = 0

    try:
        # Marqueur de chargement : on supprime d'abord les lignes du jour
        # pour rendre le pipeline idempotent
        cur.execute(
            "DELETE FROM NYC_TAXI.RAW.YELLOW_TRIPS WHERE pickup_date = %s",
            (execution_date,),
        )
        logger.info(f"DELETE : {cur.rowcount} anciennes lignes du {execution_date}")

        for key in files:
            local_file = download_to_tmp(key)

            # PUT dans le stage interne
            cur.execute(
                f"PUT file://{local_file} @NYC_TAXI.RAW.LAKE_STAGE/{execution_date}/ "
                "AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
            )
            logger.info(f"PUT OK : {local_file.name}")

            # COPY INTO
            copy_sql = f"""
                COPY INTO NYC_TAXI.RAW.YELLOW_TRIPS (
                    vendorid, tpep_pickup_datetime, tpep_dropoff_datetime,
                    passenger_count, trip_distance, ratecodeid, store_and_fwd_flag,
                    pulocationid, dolocationid, payment_type,
                    fare_amount, extra, mta_tax, tip_amount, tolls_amount,
                    improvement_surcharge, total_amount, congestion_surcharge,
                    trip_duration_min, avg_speed_mph, price_per_mile, tip_pct,
                    pickup_date, pickup_year, pickup_month, pickup_day,
                    pickup_hour, pickup_dayofweek, is_weekend
                )
                FROM (
                    SELECT
                        $1:vendorid::INTEGER,
                        $1:tpep_pickup_datetime::TIMESTAMP_NTZ,
                        $1:tpep_dropoff_datetime::TIMESTAMP_NTZ,
                        $1:passenger_count::INTEGER,
                        $1:trip_distance::FLOAT,
                        $1:ratecodeid::INTEGER,
                        $1:store_and_fwd_flag::STRING,
                        $1:pulocationid::INTEGER,
                        $1:dolocationid::INTEGER,
                        $1:payment_type::INTEGER,
                        $1:fare_amount::FLOAT,
                        $1:extra::FLOAT,
                        $1:mta_tax::FLOAT,
                        $1:tip_amount::FLOAT,
                        $1:tolls_amount::FLOAT,
                        $1:improvement_surcharge::FLOAT,
                        $1:total_amount::FLOAT,
                        $1:congestion_surcharge::FLOAT,
                        $1:trip_duration_min::FLOAT,
                        $1:avg_speed_mph::FLOAT,
                        $1:price_per_mile::FLOAT,
                        $1:tip_pct::FLOAT,
                        $1:pickup_date::DATE,
                        $1:pickup_year::INTEGER,
                        $1:pickup_month::INTEGER,
                        $1:pickup_day::INTEGER,
                        $1:pickup_hour::INTEGER,
                        $1:pickup_dayofweek::INTEGER,
                        $1:is_weekend::BOOLEAN
                    FROM @NYC_TAXI.RAW.LAKE_STAGE/{execution_date}/{local_file.name}
                )
                FILE_FORMAT = (FORMAT_NAME = NYC_TAXI.RAW.PARQUET_FORMAT)
                ON_ERROR = 'CONTINUE';
            """
            cur.execute(copy_sql)
            for row in cur.fetchall():
                logger.info(f"COPY result : {row}")

            # Lecture du nombre de lignes chargées
            cur.execute("""
                SELECT COUNT(*) FROM NYC_TAXI.RAW.YELLOW_TRIPS
                WHERE pickup_date = %s
            """, (execution_date,))
            total_rows = cur.fetchone()[0]

            local_file.unlink(missing_ok=True)

        logger.success(f"=== {total_rows:,} lignes en table RAW pour {execution_date} ===")
        return total_rows

    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        sys.exit("Usage: load_to_snowflake.py <YYYY-MM-DD>")
    load_to_snowflake(sys.argv[1])
