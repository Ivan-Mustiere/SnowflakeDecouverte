# Historique des Modifications - Stack Airflow + Spark + MinIO

Ce document récapitule les changements critiques effectués pour stabiliser l'environnement Docker sur Ubuntu.

## 1. Corrections d'Images Docker (Spark)
- **Problème** : L'image `bitnami/spark:3.5.3` renvoyait une erreur `manifest unknown`.
- **Modification** : Remplacement par `apache/spark:3.5.3` (image officielle) ou `bitnami/spark:3.5.1-debian-11-r0` pour assurer la disponibilité du manifest.

## 2. Résolution du Conflit de Port (Airflow Webserver)
- **Problème** : Erreur `Bind for 0.0.0.0:8080 failed`. Le port 8080 était déjà occupé par un projet existant (`5_mlops`).
- **Modification** : 
    - Mapping externe modifié : de `8080:8080` à `8085:8080`.
    - **Nouvelle URL d'accès** : `http://localhost:8085`.

## 3. Alignement des Dépendances Python (Airflow / Pip)
Ajustements nécessaires dans `_PIP_ADDITIONAL_REQUIREMENTS` pour résoudre les conflits de versions :

- **Pandas** : Fixé à `pandas>=2.1.2,<2.2.0`.
    - *Raison* : Le provider Snowflake (`apache-airflow-providers-snowflake==5.7.1`) refuse Pandas 2.2+.
- **dbt-snowflake** : Fixé à `1.8.3`.
    - *Raison* : La version `1.8.7` n'existe pas sur PyPI pour ce connecteur spécifique.
- **dbt-core** : Maintenu à `1.8.7` (compatible avec dbt-snowflake 1.8.3).

## 4. Maintenance du Système
- Exécution de `docker compose down --volumes --remove-orphans` pour réinitialiser proprement l'environnement.
- Initialisation réussie de la base de données via `airflow-init`.

## État Actuel des Services
- **Airflow UI** : `http://localhost:8085` (admin/admin)
- **MinIO Console** : `http://localhost:9001`
- **Spark Master** : `http://localhost:8081`