#!/usr/bin/env bash
# run.sh — Lancement et debug du pipeline NYC Taxi
set -euo pipefail

COMPOSE="docker compose"
DATE="${2:-2018-01-15}"

usage() {
  cat <<EOF
Usage: ./run.sh <commande> [date]

  start         Démarre toute la stack (docker compose up -d)
  stop          Arrête la stack
  status        État des services (docker compose ps)
  init          Initialisation 1ère fois (airflow-init + chmod docker.sock)
  logs [svc]    Logs d'un service (airflow-webserver par défaut)

  --- Briques en isolation ---
  ingest [date] Ingestion API → MinIO
  spark  [date] Spark clean/enrich (MinIO raw → staging)
  load   [date] Chargement Snowflake (MinIO staging → RAW.YELLOW_TRIPS)
  dbt           Run dbt complet (deps + seed + run + test)
  dbt-run       dbt run uniquement
  dbt-test      dbt test uniquement

  --- Pipeline complet ---
  pipeline [date]  Toutes les briques en séquence

  Date par défaut : 2018-01-15
EOF
}

check_env() {
  if [ ! -f .env ]; then
    echo "[ERREUR] Fichier .env manquant — copier .env.example et renseigner les credentials"
    echo "         cp .env.example .env"
    exit 1
  fi
}

check_docker() {
  if ! docker info &>/dev/null; then
    echo "[INFO] Correction des permissions Docker socket..."
    sudo chmod 666 /var/run/docker.sock
  fi
}

cmd_start() {
  check_env
  check_docker
  $COMPOSE up -d
  echo ""
  echo "Stack démarrée :"
  echo "  Airflow  → http://localhost:8085  (admin/admin)"
  echo "  MinIO    → http://localhost:9001  (minioadmin/minioadmin123)"
  echo "  Spark    → http://localhost:8081"
}

cmd_stop() {
  $COMPOSE down
}

cmd_status() {
  $COMPOSE ps
}

cmd_init() {
  check_env
  check_docker
  echo "[1/3] Création du dossier scheduler/logs..."
  mkdir -p logs/scheduler && chmod 777 logs/scheduler
  echo "[2/3] Initialisation Airflow..."
  $COMPOSE up airflow-init
  echo "[3/3] Démarrage de la stack..."
  $COMPOSE up -d
  echo ""
  echo "Init terminé. Pense à exécuter sf_loader/01_setup.sql dans l'UI Snowflake."
}

cmd_logs() {
  local svc="${1:-airflow-webserver}"
  $COMPOSE logs -f "$svc"
}

cmd_ingest() {
  echo "[INGEST] date=$DATE"
  $COMPOSE exec airflow-webserver python -m ingestion.extract_api "$DATE"
}

cmd_spark() {
  echo "[SPARK] date=$DATE"
  $COMPOSE exec --user root spark-master bash -c \
    "mkdir -p /home/spark/.ivy2/cache && chown -R spark:spark /home/spark"
  $COMPOSE exec spark-master /opt/spark/bin/spark-submit \
    --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
    /opt/spark_jobs/clean_and_enrich.py "$DATE"
}

cmd_load() {
  echo "[LOAD SNOWFLAKE] date=$DATE"
  $COMPOSE exec airflow-webserver python /opt/airflow/sf_loader/load_to_snowflake.py "$DATE"
}

cmd_dbt() {
  echo "[DBT] deps + seed + run + test"
  $COMPOSE exec airflow-webserver bash -c \
    "cd /opt/airflow/dbt_project && dbt deps --profiles-dir . && dbt seed --profiles-dir . && dbt run --profiles-dir . && dbt test --profiles-dir ."
}

cmd_dbt_run() {
  echo "[DBT RUN]"
  $COMPOSE exec airflow-webserver bash -c \
    "cd /opt/airflow/dbt_project && dbt run --profiles-dir ."
}

cmd_dbt_test() {
  echo "[DBT TEST]"
  $COMPOSE exec airflow-webserver bash -c \
    "cd /opt/airflow/dbt_project && dbt test --profiles-dir ."
}

cmd_pipeline() {
  echo "===== PIPELINE COMPLET — date=$DATE ====="
  cmd_ingest
  cmd_spark
  cmd_load
  cmd_dbt
  echo "===== PIPELINE TERMINE ====="
}

CMD="${1:-help}"

case "$CMD" in
  start)    cmd_start ;;
  stop)     cmd_stop ;;
  status)   cmd_status ;;
  init)     cmd_init ;;
  logs)     cmd_logs "${2:-airflow-webserver}" ;;
  ingest)   cmd_ingest ;;
  spark)    cmd_spark ;;
  load)     cmd_load ;;
  dbt)      cmd_dbt ;;
  dbt-run)  cmd_dbt_run ;;
  dbt-test) cmd_dbt_test ;;
  pipeline) cmd_pipeline ;;
  help|--help|-h) usage ;;
  *) echo "Commande inconnue : $CMD" && usage && exit 1 ;;
esac
