# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Workflow de Synchronisation
- Les modifications manuelles de l'utilisateur sont listées dans `changes.md`.
- **Règle impérative** : Dès que tu as appliqué et répercuté les modifications de `changes.md` dans les fichiers claude, tu dois supprimer le contenu traité de `changes.md` pour maintenir le fichier vide ou à jour.

## Project

NYC Yellow Taxi end-to-end data pipeline — Master 2 Data Engineer. **Soutenance : 19 mai 2026.**

Flow: `Socrata API → MinIO (Parquet) → Spark (clean/enrich) → Snowflake RAW → DBT (star schema)` — orchestré Airflow, tout en Docker.

## Stack

| Layer | Tool | Version |
|---|---|---|
| Conteneurisation | Docker Compose | 2.x |
| Orchestration | Apache Airflow | 2.10.3-python3.11 |
| Data Lake | MinIO | latest (S3-compatible) |
| Processing | PySpark | 3.5.3 (apache/spark) |
| Warehouse | Snowflake | cloud (essai gratuit OK) |
| Transformation | DBT + dbt_utils | 1.8.7 / 1.3.0 |

## Conventions

**Python**
- Type hints sur toutes les fonctions : `def run_ingestion(date_str: str) -> dict`
- Logs via `loguru.logger` — jamais `print()` sauf dans les Spark jobs (loguru indispo)
- `snake_case` pour variables, fonctions, fichiers
- Config via `os.getenv()` uniquement — jamais de valeur en dur dans le code
- Ne jamais utiliser `execution_date` comme nom de paramètre dans les `@task` Airflow (mot réservé)

**SQL / DBT**
- Tout en lowercase (`select`, `from`, `where`, `join`)
- CTEs nommées : `source` → `renamed`/`enriched` → select final
- Jamais de table hardcodée : `{{ ref('model') }}` ou `{{ source('schema', 'table') }}`
- Clé technique : `{{ dbt_utils.generate_surrogate_key([...]) }}`
- Seeds : toujours référencer via `{{ ref('seed_name') }}`, jamais via `{{ source(...) }}`

**Naming DBT**
- `stg_*` : vue sur RAW, renommage uniquement
- `dim_*` : tables de référence (`materialized='table'`)
- `fact_*` : table de faits (`materialized='table'`, `cluster_by` sur la date)
- Chaque modèle a ses tests dans `schema.yml` (unique + not_null au minimum)

**Workflow**
- Un commit = une brique fonctionnelle (ingestion OK → commit, Spark OK → commit…)
- `git push --force` interdit
- Ne jamais committer `.env` ni `dbt_project/profiles.yml` (déjà gitignorés)
- `dbt test` obligatoire après toute modification d'un modèle DBT

## Lancer la stack

```bash
cp .env.example .env            # remplir SNOWFLAKE_* et NYC_APP_TOKEN
sudo chmod 666 /var/run/docker.sock   # accès Docker socket pour Airflow
docker compose up airflow-init  # 1ère fois uniquement
docker compose up -d
docker compose ps               # vérifier que tout est healthy
```

Airflow → http://localhost:8085 (admin/admin) | MinIO → http://localhost:9001 | Spark → http://localhost:8081

Avant le 1er run : exécuter `sf_loader/01_setup.sql` dans l'UI Snowflake (rôle ACCOUNTADMIN).

Connexion Spark dans Airflow UI → Admin → Connections (non utilisée — Spark lancé via docker exec) :
- Conn Id: `spark_default` | Type: `Spark` | Host: `spark://spark-master` | Port: `7077`

## Authentification Snowflake

Authentification par clé RSA (MFA incompatible avec les connexions programmatiques).

- Clé privée : `snowflake_key.p8` (gitignorée, montée dans `/opt/airflow/snowflake_key.p8`)
- Variable `.env` : `SNOWFLAKE_PRIVATE_KEY_PATH=/opt/airflow/snowflake_key.p8`
- Clé publique appliquée via : `ALTER USER <USER> SET RSA_PUBLIC_KEY='...'` dans Snowflake UI

## Dataset

- **Source** : Socrata API, dataset `t29m-gskq`
- **Données disponibles** : 2018 (112M lignes), pas de données 2024+
- **Date de démo** : `2018-01-15` (200 000 lignes extraites, 195 847 après nettoyage Spark)
- `dim_date` couvre 2018-01-01 → 2019-12-31 (730 jours)

## Debugger chaque brique en isolation

```bash
# 1. Ingestion (API → MinIO)
docker compose exec airflow-webserver python -m ingestion.extract_api 2018-01-15

# 2. Spark (RAW → STAGING dans MinIO)
docker compose exec --user root spark-master bash -c \
  "mkdir -p /home/spark/.ivy2/cache && chown -R spark:spark /home/spark"
docker compose exec spark-master /opt/spark/bin/spark-submit \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262 \
  /opt/spark_jobs/clean_and_enrich.py 2018-01-15

# 3. Load Snowflake (MinIO staging → RAW.YELLOW_TRIPS)
docker compose exec airflow-webserver python /opt/airflow/sf_loader/load_to_snowflake.py 2018-01-15

# 4. DBT
docker compose exec airflow-webserver bash -c "cd /opt/airflow/dbt_project && dbt deps --profiles-dir . && dbt seed --profiles-dir . && dbt run --profiles-dir ."

# 4b. DBT tests uniquement
docker compose exec airflow-webserver bash -c "cd /opt/airflow/dbt_project && dbt test --profiles-dir ."
```

## DAG Airflow

Trigger manuel uniquement (`schedule=None`). Dans l'UI → Trigger DAG → Configuration JSON :
```json
{ "logical_date": "2018-01-15" }
```
Chaîne : `extract_api → spark_clean → load_snowflake → dbt_run → dbt_test → log_metrics`

Le step `spark_clean` utilise `docker exec nyc_spark_master` (BashOperator) — pas de SparkSubmitOperator.

## Soutenance (19 mai 2026)

Déroulé prévu :
1. Présenter `docs/architecture.svg` — expliquer le flux et les choix techniques (2 min)
2. `docker compose up -d` en live + vérification `docker compose ps`
3. Trigger DAG `nyc_taxi_pipeline` sur `2018-01-15` via UI Airflow
4. Suivi en direct : logs Airflow → MinIO Console (raw/ puis staging/) → Spark UI → lignes Snowflake
5. Requêtes analytiques dans Snowflake sur `MARTS.FACT_TRIPS` + jointures dimensions
6. Montrer `NYC_TAXI.RAW.PIPELINE_RUNS` (monitoring par étape)

Points forts à souligner : idempotence (DELETE + reload), bridge MinIO→Snowflake via stage interne, modèle en étoile avec surrogate keys, authentification RSA Snowflake.

## Issues connues — toutes résolues

1. ~~`dbt-snowflake==1.8.7` manquant~~ ✓
2. ~~`dbt_project/profiles.yml` manquant~~ ✓
3. ~~Conflit namespace `snowflake`~~ ✓ (dossier renommé `sf_loader/`)
4. ~~`SparkSubmitOperator` env_vars~~ ✓ (remplacé par BashOperator + docker exec)
5. ~~`taxi_zones.csv` incomplet~~ ✓ (265 zones officielles TLC)
6. ~~`dim_date` couvre 2020→2024~~ ✓ (étendu à 2018-2019)
7. ~~MFA Snowflake bloque les connexions~~ ✓ (authentification RSA)
8. ~~`execution_date` réservé dans @task~~ ✓ (renommé `date_str`)
9. ~~Schema DBT préfixé `STAGING_marts`~~ ✓ (macro `generate_schema_name` ajoutée)
10. ~~`dim_zone` référence source au lieu du seed~~ ✓ (`ref('taxi_zones')`)

## Notes d'infrastructure

- `AIRFLOW_UID=1000` dans `.env` (UID de l'utilisateur hôte WSL — ne pas remettre 50000)
- `sudo chmod 666 /var/run/docker.sock` requis au démarrage (accès Docker depuis Airflow)
- `group_add: ["989"]` dans docker-compose pour rendre le chmod permanent au prochain restart
- `logs/scheduler/` doit exister avec permissions 777 avant `docker compose up airflow-init`
- Le dossier Snowflake local s'appelle `sf_loader/` (pas `snowflake/`) pour éviter le conflit avec le package pip
- Volume `spark-ivy-cache` monté sur `/home/spark/.ivy2` pour persister les JARs Hadoop

@.claude/rules/spark.md
@.claude/rules/dbt.md
