# /demo — Préparer la démo de soutenance (19 mai 2026)

Séquence complète pour une démo propre et reproductible.

## Reset propre (J-1 ou le matin)

```bash
# Arrêter et nettoyer les conteneurs (sans toucher aux volumes MinIO/Postgres)
docker compose down

# Optionnel : reset complet des données (si on repart de zéro)
# docker compose down -v   ← ATTENTION : efface MinIO et Postgres

# Relancer
docker compose up airflow-init   # si reset complet uniquement
docker compose up -d
docker compose ps                # attendre que tout soit healthy (~60s)
```

## Vérifications pré-démo

```bash
# 1. Stack healthy
docker compose ps

# 2. .env rempli
grep SNOWFLAKE_ACCOUNT .env

# 3. Snowflake joignable (si snowflake-connector installé en local)
python3 -c "import snowflake.connector; print('OK')"
```

Dans Snowflake, vérifier que `COMPUTE_WH` existe et `NYC_TAXI` est initialisé :
```sql
SHOW WAREHOUSES LIKE 'COMPUTE_WH';
SHOW SCHEMAS IN DATABASE NYC_TAXI;
```

## Séquence de démo (15-20 min)

### 1. Architecture (2 min)
Ouvrir `docs/architecture.svg`. Décrire le flux : API → MinIO → Spark → Snowflake → DBT.
Points techniques à mentionner : Parquet Snappy, partitionnement year/month/day, stage interne Snowflake, surrogate keys DBT.

### 2. Démarrage stack (1 min)
```bash
docker compose up -d
docker compose ps
```
Montrer les 7 services : postgres, minio, minio-init, spark-master, spark-worker, airflow-webserver, airflow-scheduler.

### 3. Trigger DAG (1 min)
Airflow UI → http://localhost:8080 → activer `nyc_taxi_pipeline`
→ Trigger DAG w/ config :
```json
{ "logical_date": "2024-01-15" }
```

### 4. Suivi en direct (~10 min pendant l'exécution)

Alterner entre :
- **Airflow Graph View** : progression extract → spark → load → dbt_run → dbt_test → log_metrics
- **MinIO Console** http://localhost:9001 : apparition du fichier dans `raw/` puis `staging/`
- **Spark UI** http://localhost:8081 : job actif pendant `spark_clean`
- **Snowflake** après `load_snowflake` :
  ```sql
  SELECT COUNT(*) FROM NYC_TAXI.RAW.YELLOW_TRIPS WHERE pickup_date = '2024-01-15';
  ```

### 5. Requêtes analytiques (3-4 min)

```sql
-- Top 5 zones de prise en charge
SELECT z.zone_name, z.borough, COUNT(*) AS nb_trips, ROUND(AVG(f.total_amount), 2) AS avg_fare
FROM NYC_TAXI.MARTS.FACT_TRIPS f
JOIN NYC_TAXI.MARTS.DIM_ZONE z ON f.pickup_zone_id = z.zone_id
GROUP BY z.zone_name, z.borough
ORDER BY nb_trips DESC
LIMIT 5;

-- Pourboire moyen par heure
SELECT f.pickup_hour, ROUND(AVG(f.tip_pct), 1) AS avg_tip_pct, COUNT(*) AS nb_trips
FROM NYC_TAXI.MARTS.FACT_TRIPS f
GROUP BY f.pickup_hour
ORDER BY f.pickup_hour;

-- Revenu par vendor et mode de paiement
SELECT v.vendor_name, p.payment_type_name, ROUND(SUM(f.total_amount), 0) AS revenue
FROM NYC_TAXI.MARTS.FACT_TRIPS f
JOIN NYC_TAXI.MARTS.DIM_VENDOR v ON f.vendor_id = v.vendor_id
JOIN NYC_TAXI.MARTS.DIM_PAYMENT p ON f.payment_type_id = p.payment_type_id
GROUP BY v.vendor_name, p.payment_type_name
ORDER BY revenue DESC;
```

### 6. Monitoring (1 min)

```sql
SELECT * FROM NYC_TAXI.RAW.PIPELINE_RUNS ORDER BY created_at DESC LIMIT 10;
```

Montrer : run_id, étapes, statuts, durées, rows_processed.

## Points techniques à préparer (questions jury)

- **Pourquoi MinIO et pas S3 ?** → reproductible localement, même API boto3
- **Pourquoi un stage interne Snowflake ?** → Snowflake cloud ne peut pas accéder à un MinIO local
- **Idempotence ?** → DELETE par pickup_date avant COPY INTO + Spark overwrite
- **Pourquoi Spark pour 200k lignes ?** → démo de la techno ; en prod les données sont ~10M/jour
- **Surrogate key ?** → évite les doublons si re-run, clé stable indépendante de l'ordre d'insertion
