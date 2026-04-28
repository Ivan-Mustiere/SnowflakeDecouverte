# /test-pipeline — Tester chaque brique en isolation

Exécuter dans l'ordre. Chaque étape valide le prérequis de la suivante.

## Prérequis

```bash
docker compose ps   # vérifier que tous les services sont healthy
```

MinIO doit avoir le bucket `nyc-taxi-lake` (créé automatiquement par `minio-init`).

## Étape 1 — Ingestion (API Socrata → MinIO raw/)

```bash
docker compose exec airflow-webserver python -m ingestion.extract_api 2024-01-15
```

**Validation** : ouvrir MinIO Console (http://localhost:9001) et vérifier :
`nyc-taxi-lake/raw/yellow_taxi/year=2024/month=01/day=15/trips_2024-01-15.parquet`

Log attendu : `Extraction terminée : X lignes` puis `Upload OK`.

## Étape 2 — Spark (MinIO raw/ → MinIO staging/)

```bash
docker compose exec spark-master spark-submit \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  /opt/spark_jobs/clean_and_enrich.py 2024-01-15
```

**Validation** :
- Spark UI http://localhost:8081 → 1 application completed
- MinIO : `nyc-taxi-lake/staging/yellow_taxi/year=2024/month=01/day=15/`
- Log attendu : `[CLEAN] N -> M lignes` puis `[DONE] M lignes écrites`

## Étape 3 — Load Snowflake (MinIO staging/ → RAW.YELLOW_TRIPS)

```bash
docker compose exec airflow-webserver \
  python /opt/airflow/sf_loader/load_to_snowflake.py 2024-01-15
```

**Validation** dans Snowflake :
```sql
SELECT COUNT(*) FROM NYC_TAXI.RAW.YELLOW_TRIPS WHERE pickup_date = '2024-01-15';
-- Doit retourner > 0
```

## Étape 4 — DBT (RAW → MARTS)

```bash
# Installer dépendances + seed + run
docker compose exec airflow-webserver bash -c \
  "cd /opt/airflow/dbt_project && \
   dbt deps --profiles-dir . && \
   dbt seed --profiles-dir . && \
   dbt run --profiles-dir ."

# Tests qualité
docker compose exec airflow-webserver bash -c \
  "cd /opt/airflow/dbt_project && dbt test --profiles-dir ."
```

**Validation** dans Snowflake :
```sql
SELECT COUNT(*) FROM NYC_TAXI.MARTS.FACT_TRIPS;
SELECT COUNT(*) FROM NYC_TAXI.MARTS.DIM_DATE;    -- 1827 (couverture 2020-2024)
SELECT COUNT(*) FROM NYC_TAXI.MARTS.DIM_ZONE;    -- 265
SELECT COUNT(*) FROM NYC_TAXI.MARTS.DIM_VENDOR;  -- 4
SELECT COUNT(*) FROM NYC_TAXI.MARTS.DIM_PAYMENT; -- 7
```

Tous les tests DBT doivent passer (0 erreur).

## Étape 5 — Pipeline complet via DAG

Dans Airflow UI → activer `nyc_taxi_pipeline` → Trigger DAG w/ config :
```json
{ "logical_date": "2024-01-15" }
```

Suivre dans Graph View. En cas d'erreur : logs de la tâche → identifier l'étape → re-tester en isolation.
