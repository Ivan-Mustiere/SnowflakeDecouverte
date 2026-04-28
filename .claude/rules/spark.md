# Règles Spark — s'appliquent à spark_jobs/**

## Configuration S3A pour MinIO (obligatoire)

Toute SparkSession qui lit/écrit sur MinIO doit avoir ces configs :

```python
spark = (
    SparkSession.builder
    .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT", "http://minio:9000"))
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ACCESS_KEY", "minioadmin"))
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_SECRET_KEY", "minioadmin123"))
    .config("spark.hadoop.fs.s3a.path.style.access", "true")   # obligatoire pour MinIO
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)
```

Les packages hadoop-aws + aws-java-sdk-bundle sont passés via `--packages` dans le `spark-submit`.

## Écriture Parquet

```python
# Toujours overwrite pour l'idempotence
df.coalesce(1).write.mode("overwrite").parquet(output_path)
```

`coalesce(1)` est acceptable pour la démo (volume ~200k lignes). En production, supprimer.

## Ce qu'il ne faut pas faire

- `df.collect()` sur un dataset entier → OOM garanti sur des millions de lignes
- `df.toPandas()` sans filtre préalable → idem
- `df.show(n)` en prod → acceptable uniquement en debug local
- Config Spark en dur dans le code → toujours via `os.getenv()`

## Gestion mémoire

Worker configuré à `SPARK_WORKER_MEMORY=2G` + `SPARK_WORKER_CORES=2` (docker-compose.yml).
Si le job OOM : réduire `MAX_ROWS` dans `extract_api.py` ou augmenter la mémoire worker.

## Filtres de nettoyage (clean_and_enrich.py)

Les filtres actuels dans `clean()` :
- `trip_distance` : 0 < d < 200 miles
- `total_amount` : 0 < t < 1000
- `passenger_count` : 1 à 8 (max légal NYC)
- `tpep_dropoff > tpep_pickup`

Post-enrichissement dans `enrich()` :
- `trip_duration_min` : 1 min à 600 min (10h)
- `avg_speed_mph` < 100

Ne pas assouplir ces filtres sans raison — ils garantissent la cohérence du modèle analytique.
