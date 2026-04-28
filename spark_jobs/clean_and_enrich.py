"""
Job Spark : RAW -> STAGING

Lit le Parquet brut depuis MinIO, nettoie, gère les valeurs aberrantes,
enrichit (durée du trajet, vitesse moyenne, prix au km), et réécrit
en Parquet partitionné dans /staging/.

Usage (depuis le conteneur Spark) :
    spark-submit \
        --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
        /opt/spark_jobs/clean_and_enrich.py 2024-01-15
"""

from __future__ import annotations

import os
import sys

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DoubleType, IntegerType, TimestampType


# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "nyc-taxi-lake")


# -----------------------------------------------------------------------------
# Spark session avec config S3A pour MinIO
# -----------------------------------------------------------------------------
def build_spark(app_name: str = "nyc_taxi_clean") -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.sql.session.timeZone", "America/New_York")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# -----------------------------------------------------------------------------
# Transformations
# -----------------------------------------------------------------------------
def cast_columns(df):
    """Conversion des types (Socrata renvoie tout en string)."""
    casts = {
        "vendorid": IntegerType(),
        "passenger_count": IntegerType(),
        "trip_distance": DoubleType(),
        "ratecodeid": IntegerType(),
        "pulocationid": IntegerType(),
        "dolocationid": IntegerType(),
        "payment_type": IntegerType(),
        "fare_amount": DoubleType(),
        "extra": DoubleType(),
        "mta_tax": DoubleType(),
        "tip_amount": DoubleType(),
        "tolls_amount": DoubleType(),
        "improvement_surcharge": DoubleType(),
        "total_amount": DoubleType(),
        "congestion_surcharge": DoubleType(),
        "tpep_pickup_datetime": TimestampType(),
        "tpep_dropoff_datetime": TimestampType(),
    }
    for col, t in casts.items():
        if col in df.columns:
            df = df.withColumn(col, F.col(col).cast(t))
    return df


def clean(df):
    """Filtre les valeurs aberrantes et impossibles."""
    before = df.count()

    df = df.filter(
        (F.col("tpep_pickup_datetime").isNotNull())
        & (F.col("tpep_dropoff_datetime").isNotNull())
        & (F.col("tpep_dropoff_datetime") > F.col("tpep_pickup_datetime"))
        # Distance : 0 < d < 200 miles
        & (F.col("trip_distance") > 0) & (F.col("trip_distance") < 200)
        # Montant total positif
        & (F.col("total_amount") > 0) & (F.col("total_amount") < 1000)
        # Passagers : 1 à 8 (8 = max légal NYC)
        & (F.col("passenger_count").between(1, 8))
    )

    after = df.count()
    print(f"[CLEAN] {before:,} -> {after:,} lignes ({before - after:,} supprimées)")
    return df


def enrich(df):
    """Ajout de colonnes calculées utiles à l'analyse."""
    df = (
        df
        # Durée en minutes
        .withColumn(
            "trip_duration_min",
            (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime")) / 60,
        )
        # Vitesse moyenne en mph
        .withColumn(
            "avg_speed_mph",
            F.when(F.col("trip_duration_min") > 0,
                   F.col("trip_distance") / (F.col("trip_duration_min") / 60))
             .otherwise(None),
        )
        # Prix au mile
        .withColumn(
            "price_per_mile",
            F.when(F.col("trip_distance") > 0,
                   F.col("total_amount") / F.col("trip_distance"))
             .otherwise(None),
        )
        # % pourboire
        .withColumn(
            "tip_pct",
            F.when(F.col("fare_amount") > 0,
                   (F.col("tip_amount") / F.col("fare_amount")) * 100)
             .otherwise(0.0),
        )
        # Dimensions temporelles
        .withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
        .withColumn("pickup_year", F.year("tpep_pickup_datetime"))
        .withColumn("pickup_month", F.month("tpep_pickup_datetime"))
        .withColumn("pickup_day", F.dayofmonth("tpep_pickup_datetime"))
        .withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
        .withColumn("pickup_dayofweek", F.dayofweek("tpep_pickup_datetime"))
        .withColumn("is_weekend", F.col("pickup_dayofweek").isin(1, 7))
    )

    # Filtre des aberrations détectées après calcul
    df = df.filter(
        (F.col("trip_duration_min").between(1, 600))   # 1 min à 10 h
        & (F.col("avg_speed_mph") < 100)               # vitesse plausible
    )
    return df


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
def run(execution_date: str) -> None:
    """
    Args:
        execution_date: 'YYYY-MM-DD'
    """
    year, month, day = execution_date.split("-")
    input_path = (
        f"s3a://{MINIO_BUCKET}/raw/yellow_taxi/"
        f"year={year}/month={month}/day={day}/"
    )
    output_path = (
        f"s3a://{MINIO_BUCKET}/staging/yellow_taxi/"
        f"year={year}/month={month}/day={day}/"
    )

    spark = build_spark(f"nyc_taxi_clean_{execution_date}")
    print(f"[READ ] {input_path}")
    df = spark.read.parquet(input_path)
    print(f"[READ ] schéma : {df.dtypes}")

    df = cast_columns(df)
    df = clean(df)
    df = enrich(df)

    print(f"[WRITE] {output_path}")
    df.coalesce(1).write.mode("overwrite").parquet(output_path)

    print(f"[DONE ] {df.count():,} lignes écrites")
    spark.stop()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.exit("Usage: clean_and_enrich.py <YYYY-MM-DD>")
    run(sys.argv[1])
