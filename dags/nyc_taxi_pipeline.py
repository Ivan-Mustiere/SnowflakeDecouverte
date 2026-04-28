"""
DAG : nyc_taxi_pipeline

Orchestration end-to-end :
    1. extract_api      : NYC Socrata API -> MinIO (raw)
    2. spark_clean      : Spark RAW -> STAGING dans MinIO
    3. load_snowflake   : MinIO staging -> Snowflake RAW
    4. dbt_run          : DBT staging + marts
    5. dbt_test         : tests qualité DBT
    6. log_metrics      : insert dans pipeline_runs (monitoring)

Trigger manuel :
    Airflow UI -> Trigger DAG -> Configuration JSON :
    { "logical_date": "2018-01-15" }
"""

from __future__ import annotations

import os
import sys
import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator

sys.path.insert(0, "/opt/airflow")

from ingestion.extract_api import run_ingestion              # noqa: E402
from sf_loader.load_to_snowflake import load_to_snowflake    # noqa: E402


DEFAULT_ARGS = {
    "owner": "ivan",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

DBT_PROJECT_DIR  = "/opt/airflow/dbt_project"
DBT_PROFILES_DIR = "/opt/airflow/dbt_project"

# Date par défaut si le DAG est triggeré sans config
DEFAULT_DATE = "2018-01-15"


# -----------------------------------------------------------------------------
# Tasks
# -----------------------------------------------------------------------------
@task(task_id="extract_api")
def extract_task(date_str: str) -> dict:
    start = time.time()
    result = run_ingestion(date_str)
    result["duration_sec"] = round(time.time() - start, 2)
    return result


@task(task_id="load_snowflake")
def load_task(date_str: str) -> dict:
    start = time.time()
    rows = load_to_snowflake(date_str)
    return {
        "rows": rows,
        "duration_sec": round(time.time() - start, 2),
        "date": date_str,
    }


@task(task_id="log_metrics")
def log_metrics_task(extract_result: dict, load_result: dict) -> None:
    import snowflake.connector
    from cryptography.hazmat.primitives import serialization

    key_path = os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH", "/opt/airflow/snowflake_key.p8")
    with open(key_path, "rb") as f:
        private_key = serialization.load_pem_private_key(f.read(), password=None)
    private_key_bytes = private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        private_key=private_key_bytes,
        role=os.getenv("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        database=os.getenv("SNOWFLAKE_DATABASE", "NYC_TAXI"),
        schema="RAW",
    )
    cur = conn.cursor()
    run_id = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    exec_date = load_result["date"]

    records = [
        (run_id, exec_date, "extract", "OK", extract_result.get("rows", 0),
         extract_result.get("duration_sec", 0), None),
        (run_id, exec_date, "load",    "OK", load_result.get("rows", 0),
         load_result.get("duration_sec", 0), None),
    ]
    cur.executemany("""
        INSERT INTO NYC_TAXI.RAW.PIPELINE_RUNS
        (run_id, execution_date, step, status, rows_processed, duration_sec, error_message)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, records)
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
    schedule=None,          # déclenchement manuel uniquement
    start_date=datetime(2018, 1, 1),
    catchup=False,
    max_active_runs=1,
    params={"logical_date": DEFAULT_DATE},
    tags=["projet_final", "nyc_taxi", "data_engineering"],
) as dag:

    # Date lue depuis les params du trigger (ou DEFAULT_DATE)
    EXEC_DATE = "{{ params.logical_date }}"

    # 1. Extraction API
    extract = extract_task(EXEC_DATE)

    # 2. Spark clean & enrich
    spark_clean = BashOperator(
        task_id="spark_clean",
        bash_command=(
            "docker exec nyc_spark_master "
            "/opt/spark/bin/spark-submit "
            "--packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 "
            "/opt/spark_jobs/clean_and_enrich.py {{ params.logical_date }}"
        ),
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

    # 6. Log metrics
    metrics = log_metrics_task(extract, load)

    # Dépendances
    extract >> spark_clean >> load >> dbt_run >> dbt_test >> metrics
