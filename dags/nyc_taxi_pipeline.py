"""
DAG : nyc_taxi_pipeline

Orchestration end-to-end :
    1. extract_api      : NYC Socrata API -> MinIO (raw)
    2. spark_clean      : Spark RAW -> STAGING dans MinIO
    3. load_snowflake   : MinIO staging -> Snowflake RAW
    4. dbt_run          : DBT staging + marts
    5. dbt_test         : tests qualité DBT
    6. log_metrics      : insert dans pipeline_runs (monitoring)
"""

from __future__ import annotations

import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

# Permettre l'import des modules du projet
sys.path.insert(0, "/opt/airflow")

from ingestion.extract_api import run_ingestion          # noqa: E402
from sf_loader.load_to_snowflake import load_to_snowflake  # noqa: E402


# -----------------------------------------------------------------------------
# Config DAG
# -----------------------------------------------------------------------------
DEFAULT_ARGS = {
    "owner": "ivan",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

DBT_PROJECT_DIR = "/opt/airflow/dbt_project"
DBT_PROFILES_DIR = "/opt/airflow/dbt_project"


# -----------------------------------------------------------------------------
# Tasks
# -----------------------------------------------------------------------------
@task(task_id="extract_api")
def extract_task(execution_date: str) -> dict:
    """Étape 1 : ingestion API -> MinIO (raw layer)."""
    start = time.time()
    result = run_ingestion(execution_date)
    result["duration_sec"] = round(time.time() - start, 2)
    return result


@task(task_id="load_snowflake")
def load_task(execution_date: str) -> dict:
    """Étape 3 : chargement MinIO staging -> Snowflake RAW."""
    start = time.time()
    rows = load_to_snowflake(execution_date)
    return {
        "rows": rows,
        "duration_sec": round(time.time() - start, 2),
        "date": execution_date,
    }


@task(task_id="log_metrics")
def log_metrics_task(extract_result: dict, load_result: dict) -> None:
    """Étape 6 : enregistrement des métriques (bonus monitoring)."""
    import os
    import snowflake.connector

    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        database=os.getenv("SNOWFLAKE_DATABASE", "NYC_TAXI"),
        schema="RAW",
    )
    cur = conn.cursor()
    run_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    exec_date = load_result["date"]

    rows = [
        (run_id, exec_date, "extract", "OK", extract_result.get("rows", 0),
         extract_result.get("duration_sec", 0), None),
        (run_id, exec_date, "load",    "OK", load_result.get("rows", 0),
         load_result.get("duration_sec", 0), None),
    ]
    cur.executemany("""
        INSERT INTO NYC_TAXI.RAW.PIPELINE_RUNS
        (run_id, execution_date, step, status, rows_processed, duration_sec, error_message)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, rows)
    cur.close()
    conn.close()
    print(f"Métriques enregistrées pour run_id={run_id}")


# -----------------------------------------------------------------------------
# DAG
# -----------------------------------------------------------------------------
with DAG(
    dag_id="nyc_taxi_pipeline",
    default_args=DEFAULT_ARGS,
    description="Pipeline NYC Taxi : API -> MinIO -> Spark -> Snowflake -> DBT",
    schedule_interval="0 6 * * *",   # tous les jours à 6h UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["projet_final", "nyc_taxi", "data_engineering"],
) as dag:

    # Date d'exécution (à passer en YYYY-MM-DD à toutes les étapes)
    EXEC_DATE = "{{ ds }}"

    # 1. Extraction API
    extract = extract_task(EXEC_DATE)

    # 2. Spark clean & enrich
    spark_clean = SparkSubmitOperator(
        task_id="spark_clean",
        application="/opt/airflow/spark_jobs/clean_and_enrich.py",
        conn_id="spark_default",   # à créer dans Airflow UI : spark://spark-master:7077
        application_args=[EXEC_DATE],
        packages="org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
        env_vars={
            "MINIO_ENDPOINT":  "{{ var.value.get('MINIO_ENDPOINT', 'http://minio:9000') }}",
            "MINIO_ACCESS_KEY": "{{ var.value.get('MINIO_ACCESS_KEY', 'minioadmin') }}",
            "MINIO_SECRET_KEY": "{{ var.value.get('MINIO_SECRET_KEY', 'minioadmin123') }}",
            "MINIO_BUCKET":    "{{ var.value.get('MINIO_BUCKET', 'nyc-taxi-lake') }}",
        },
        verbose=True,
    )

    # 3. Load Snowflake
    load = load_task(EXEC_DATE)

    # 4. DBT run
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt deps --profiles-dir {DBT_PROFILES_DIR} && "
            f"dbt seed --profiles-dir {DBT_PROFILES_DIR} && "
            f"dbt run --profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    # 5. DBT test
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt test --profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    # 6. Log metrics (bonus monitoring)
    metrics = log_metrics_task(extract, load)

    # Dépendances
    extract >> spark_clean >> load >> dbt_run >> dbt_test >> metrics
