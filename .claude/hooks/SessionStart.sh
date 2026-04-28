#!/bin/bash
# Hook SessionStart — état du projet au démarrage de chaque session Claude Code

PROJECT_DIR="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$PROJECT_DIR" || exit 0

echo ""
echo "========================================"
echo " NYC Taxi Pipeline — Session Start"
echo "========================================"

# --- .env ---
if [ -f ".env" ]; then
    if grep -q "SNOWFLAKE_ACCOUNT=xxxxx" .env 2>/dev/null || grep -q "SNOWFLAKE_ACCOUNT=$" .env 2>/dev/null; then
        echo "[.env]     ATTENTION : credentials Snowflake non renseignés"
    else
        echo "[.env]     OK (credentials présents)"
    fi
else
    echo "[.env]     MANQUANT — copier .env.example et renseigner les credentials"
fi

# --- Docker ---
if docker info >/dev/null 2>&1; then
    RUNNING=$(docker compose ps --services --filter "status=running" 2>/dev/null | wc -l | tr -d ' ')
    TOTAL=$(docker compose ps --services 2>/dev/null | wc -l | tr -d ' ')
    if [ "$RUNNING" -gt 0 ] 2>/dev/null; then
        echo "[Docker]   $RUNNING/$TOTAL services actifs"
        docker compose ps --format "  {.Name}: {.Status}" 2>/dev/null || \
        docker compose ps 2>/dev/null | tail -n +2 | awk '{printf "  %-30s %s\n", $1, $4}'
    else
        echo "[Docker]   Stack arrêtée (lancer avec: docker compose up -d)"
    fi
else
    echo "[Docker]   Docker non disponible"
fi

# --- Git ---
if git rev-parse --git-dir >/dev/null 2>&1; then
    LAST_COMMIT=$(git log -1 --pretty=format:"%h %s (%ar)" 2>/dev/null)
    BRANCH=$(git branch --show-current 2>/dev/null)
    DIRTY=$(git status --short 2>/dev/null | wc -l | tr -d ' ')
    echo "[Git]      $BRANCH | dernier commit : $LAST_COMMIT"
    if [ "$DIRTY" -gt 0 ]; then
        echo "[Git]      $DIRTY fichier(s) modifié(s) non commités"
    fi
else
    echo "[Git]      Pas de dépôt git initialisé"
fi

echo "========================================"
echo ""
