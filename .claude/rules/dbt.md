# Règles DBT — s'appliquent à dbt_project/models/**

## Naming obligatoire

| Préfixe | Schéma cible | Matérialization | Usage |
|---|---|---|---|
| `stg_` | STAGING | view | Renommage colonnes RAW, 1 source = 1 modèle staging |
| `dim_` | MARTS | table | Référentiels (vendor, payment, zone, date) |
| `fact_` | MARTS | table | Événements mesurables (courses) |

## Matérialisation

```sql
-- Staging : toujours view
{{ config(materialized='view') }}

-- Mart dimension : table
{{ config(materialized='table') }}

-- Fact : table avec cluster
{{ config(materialized='table', cluster_by=['pickup_date_key']) }}
```

## Références entre modèles

```sql
-- ✓ Correct
select * from {{ ref('stg_trips') }}
select * from {{ source('raw', 'yellow_trips') }}

-- ✗ Interdit — hardcodé, se casse si le schéma change
select * from NYC_TAXI.STAGING.STG_TRIPS
```

## Tests obligatoires dans schema.yml

Tout nouveau modèle **doit** avoir au minimum :
- `unique` + `not_null` sur sa clé primaire
- `not_null` sur les colonnes critiques (montants, dates)
- `relationships` sur chaque foreign key de `fact_trips`

```yaml
- name: mon_modele
  columns:
    - name: ma_pk
      tests:
        - unique
        - not_null
```

## Sources

Toute nouvelle table source doit être déclarée dans `staging/sources.yml` :
```yaml
sources:
  - name: raw
    database: NYC_TAXI
    schema: RAW
    tables:
      - name: ma_table
```

## Surrogate keys

Utiliser exclusivement `dbt_utils.generate_surrogate_key()` pour les clés techniques :
```sql
{{ dbt_utils.generate_surrogate_key(['col1', 'col2', 'col3']) }} as trip_key
```
Ne jamais utiliser `MD5(CONCAT(...))` directement — comportement différent selon l'adapter.

## dim_date

Générée via `generator(rowcount => N)` de Snowflake (pas de source externe).
Couvre actuellement 2020-01-01 → 2024-12-31 (1827 jours).
Si de nouvelles dates arrivent, augmenter `rowcount` et re-run `dbt run --select dim_date`.

## Ordre de run

DBT résout les dépendances automatiquement. En cas de run sélectif :
```bash
dbt run --select +fact_trips   # lance fact_trips ET tous ses ancêtres
dbt run --select dim_date+     # lance dim_date ET tous ses descendants
```
