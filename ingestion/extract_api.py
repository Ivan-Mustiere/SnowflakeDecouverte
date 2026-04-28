"""
Ingestion des courses de taxi NYC depuis l'API Socrata vers le Data Lake (MinIO).

L'API Socrata expose les données ouvertes de NYC. Elle est paginée via $limit / $offset
et accepte un filtre SoQL sur la colonne tpep_pickup_datetime pour ne récupérer
qu'une plage de dates donnée.

Sortie : un fichier Parquet par exécution, partitionné par année/mois/jour
         dans le bucket MinIO sous /raw/yellow_taxi/.
"""

from __future__ import annotations

import io
import os
from datetime import datetime, timedelta
from typing import Optional

import boto3
import pandas as pd
import requests
from botocore.client import Config
from loguru import logger


# -----------------------------------------------------------------------------
# Configuration depuis les variables d'environnement
# -----------------------------------------------------------------------------
SOCRATA_BASE_URL = "https://data.cityofnewyork.us/resource"
DATASET_ID = os.getenv("NYC_DATASET_ID", "t29m-gskq")
APP_TOKEN = os.getenv("NYC_APP_TOKEN", "")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "nyc-taxi-lake")

PAGE_SIZE = 50_000   # max autorisé par Socrata avec un token
MAX_ROWS = 200_000   # garde-fou pour les démos ; mettre None pour tout récupérer


# -----------------------------------------------------------------------------
# Clients
# -----------------------------------------------------------------------------
def get_s3_client():
    """Retourne un client boto3 configuré pour MinIO."""
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


# -----------------------------------------------------------------------------
# Extraction
# -----------------------------------------------------------------------------
def fetch_page(offset: int, where_clause: str) -> list[dict]:
    """Récupère une page de résultats depuis l'API Socrata."""
    params = {
        "$limit": PAGE_SIZE,
        "$offset": offset,
        "$where": where_clause,
        "$order": "tpep_pickup_datetime ASC",
    }
    headers = {"X-App-Token": APP_TOKEN} if APP_TOKEN else {}
    url = f"{SOCRATA_BASE_URL}/{DATASET_ID}.json"

    logger.info(f"GET {url}  offset={offset}  where='{where_clause}'")
    resp = requests.get(url, params=params, headers=headers, timeout=60)
    resp.raise_for_status()
    return resp.json()


def extract_data(start_date: str, end_date: str) -> pd.DataFrame:
    """
    Extrait toutes les courses entre start_date et end_date (inclus -> exclu).

    Args:
        start_date: 'YYYY-MM-DD'
        end_date  : 'YYYY-MM-DD'  (exclu)
    """
    where = (
        f"tpep_pickup_datetime >= '{start_date}T00:00:00' "
        f"AND tpep_pickup_datetime < '{end_date}T00:00:00'"
    )

    all_rows: list[dict] = []
    offset = 0
    while True:
        page = fetch_page(offset, where)
        if not page:
            logger.info("Plus de données à récupérer.")
            break

        all_rows.extend(page)
        logger.info(f"Cumul : {len(all_rows):,} lignes")

        if len(page) < PAGE_SIZE:
            break
        if MAX_ROWS and len(all_rows) >= MAX_ROWS:
            logger.warning(f"MAX_ROWS={MAX_ROWS:,} atteint, arrêt.")
            all_rows = all_rows[:MAX_ROWS]
            break

        offset += PAGE_SIZE

    df = pd.DataFrame(all_rows)
    logger.success(f"Extraction terminée : {len(df):,} lignes, {len(df.columns)} colonnes")
    return df


# -----------------------------------------------------------------------------
# Chargement vers MinIO
# -----------------------------------------------------------------------------
def upload_to_minio(df: pd.DataFrame, key: str) -> str:
    """Convertit le DataFrame en Parquet et l'envoie sur MinIO."""
    s3 = get_s3_client()

    buffer = io.BytesIO()
    df.to_parquet(buffer, engine="pyarrow", index=False, compression="snappy")
    buffer.seek(0)

    s3.put_object(Bucket=MINIO_BUCKET, Key=key, Body=buffer.getvalue())
    full_path = f"s3a://{MINIO_BUCKET}/{key}"
    logger.success(f"Upload OK : {full_path}  ({buffer.tell() / 1024:.1f} KB)")
    return full_path


# -----------------------------------------------------------------------------
# Point d'entrée appelé par Airflow
# -----------------------------------------------------------------------------
def run_ingestion(execution_date: Optional[str] = None) -> dict:
    """
    Tâche principale appelée par le DAG Airflow.

    Args:
        execution_date: 'YYYY-MM-DD'. Par défaut, hier.

    Returns:
        dict avec le chemin S3 et le nombre de lignes (pushé en XCom).
    """
    if execution_date is None:
        execution_date = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")

    dt = datetime.strptime(execution_date, "%Y-%m-%d")
    next_day = (dt + timedelta(days=1)).strftime("%Y-%m-%d")

    logger.info(f"=== Ingestion pour la date {execution_date} ===")

    df = extract_data(execution_date, next_day)
    if df.empty:
        logger.warning("DataFrame vide, rien à uploader.")
        return {"path": None, "rows": 0}

    key = (
        f"raw/yellow_taxi/"
        f"year={dt.year}/month={dt.month:02d}/day={dt.day:02d}/"
        f"trips_{execution_date}.parquet"
    )
    path = upload_to_minio(df, key)

    return {"path": path, "rows": len(df), "date": execution_date}


if __name__ == "__main__":
    # Pour tester en local : python -m ingestion.extract_api 2024-01-15
    import sys
    date_arg = sys.argv[1] if len(sys.argv) > 1 else None
    result = run_ingestion(date_arg)
    print(result)
