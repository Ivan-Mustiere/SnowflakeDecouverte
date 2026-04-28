#!/bin/bash
# Hook PostToolUse — vérification syntaxique après écriture/édition de fichiers clés

# Lire le JSON envoyé par Claude Code sur stdin
INPUT=$(cat)

# Extraire file_path depuis le JSON (python3 garanti disponible)
FILE_PATH=$(printf '%s' "$INPUT" | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    ti = data.get('tool_input', {})
    print(ti.get('file_path', ''))
except Exception:
    print('')
" 2>/dev/null)

[ -z "$FILE_PATH" ] && exit 0

# Normaliser (chemin absolu → relatif au projet si possible)
BASENAME=$(basename "$FILE_PATH")
DIRPATH=$(dirname "$FILE_PATH")

# ── Python : dags/, spark_jobs/, ingestion/, snowflake/ ──────────────────────
case "$FILE_PATH" in
    */dags/*.py|*/spark_jobs/*.py|*/ingestion/*.py|*/snowflake/*.py)
        echo ""
        echo "→ [py_compile] $BASENAME"
        if python3 -m py_compile "$FILE_PATH" 2>&1; then
            echo "  ✓ Syntaxe Python OK"
        else
            echo "  ✗ Erreur de syntaxe — corriger avant de committer"
        fi
        ;;
esac

# ── DBT : models SQL ──────────────────────────────────────────────────────────
case "$FILE_PATH" in
    */dbt_project/models/*.sql|*/dbt_project/models/**/*.sql)
        echo ""
        echo "→ [DBT] Modèle modifié : $BASENAME"
        echo "  Penser à lancer : dbt test --profiles-dir /opt/airflow/dbt_project"
        echo "  (dans le conteneur airflow-webserver)"
        ;;
    */dbt_project/models/*.yml|*/dbt_project/seeds/*.csv)
        echo ""
        echo "→ [DBT] Schema/seed modifié : $BASENAME"
        echo "  Penser à lancer : dbt seed && dbt run && dbt test"
        ;;
esac

# ── docker-compose.yml ────────────────────────────────────────────────────────
case "$BASENAME" in
    docker-compose.yml)
        echo ""
        echo "→ [Docker] docker-compose.yml modifié"
        if docker compose config --quiet 2>/dev/null; then
            echo "  ✓ Syntaxe docker-compose valide"
        else
            echo "  ✗ Erreur dans docker-compose.yml"
        fi
        ;;
esac

exit 0
