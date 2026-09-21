#!/usr/bin/env bash
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

LOG_DIR="$REPO_ROOT/portal/test-results"
mkdir -p "$LOG_DIR"
SUPERVISOR_LOG="$LOG_DIR/autonomous_supervisor.log"

echo "[$(date -u +"%Y-%m-%dT%H:%M:%SZ")] Autonomous Wave 0 Supervisor Daemon started." >> "$SUPERVISOR_LOG"

DBW_BRONZE_TRIGGERED_DOWNSTREAM=0
BDL_LANDING_TRIGGERED_DOWNSTREAM=0

while true; do
    TIMESTAMP="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
    
    # Check DBW Bronze process
    DBW_PID="$(pgrep -f "src/dbw_bronze_loader.py" | head -n 1 || true)"
    if [ -n "$DBW_PID" ]; then
        echo "[$TIMESTAMP] DBW Bronze loader running (PID $DBW_PID)." >> "$SUPERVISOR_LOG"
    else
        if [ "$DBW_BRONZE_TRIGGERED_DOWNSTREAM" -eq 0 ]; then
            # Verify if bronze loader completed successfully
            if grep -q "GUS DBW Bronze transformation completed successfully" "$LOG_DIR/dbw-bronze/bronze_loader.log" 2>/dev/null; then
                DBW_COMPLETION_MARKER="$(
                    find "$LOG_DIR/dbw-bronze" -mindepth 2 -maxdepth 2 -type f \
                        -name 'bronze-complete-v1-*.json' -printf '%T@ %p\n' 2>/dev/null \
                        | sort -nr | head -n 1 | cut -d' ' -f2-
                )"
                DBW_RELEASE_ID=""
                DBW_RELEASE_DIR=""
                if [ -n "$DBW_COMPLETION_MARKER" ]; then
                    DBW_RELEASE_ID="$(basename "$DBW_COMPLETION_MARKER")"
                    DBW_RELEASE_ID="${DBW_RELEASE_ID#bronze-complete-v1-}"
                    DBW_RELEASE_ID="${DBW_RELEASE_ID%.json}"
                    DBW_RELEASE_DIR="$(basename "$(dirname "$DBW_COMPLETION_MARKER")")"
                fi
                if [[ ! "$DBW_RELEASE_ID" =~ ^[0-9a-f]{64}$ || "$DBW_RELEASE_DIR" != "$DBW_RELEASE_ID" ]]; then
                    echo "[$TIMESTAMP] DBW Bronze completion log has no matching release-bound marker; downstream models remain blocked." >> "$SUPERVISOR_LOG"
                else
                    PYTHON_BIN="$REPO_ROOT/.venv/bin/python"
                    [ -x "$PYTHON_BIN" ] || PYTHON_BIN="$(command -v python3 || true)"
                    DBT_BIN="$REPO_ROOT/.venv/bin/dbt"
                    [ -x "$DBT_BIN" ] || DBT_BIN="$(command -v dbt || true)"
                    DBW_DATA_ROOT="$LOG_DIR/dbw-dbt-data"
                    DBW_DUCKDB_PATH="$LOG_DIR/dbw-${DBW_RELEASE_ID}.duckdb"
                    mkdir -p "$DBW_DATA_ROOT"

                    if [ -z "$PYTHON_BIN" ] || [ -z "$DBT_BIN" ]; then
                        echo "[$TIMESTAMP] Python or dbt executable is unavailable; DBW downstream models remain pending." >> "$SUPERVISOR_LOG"
                    elif ! PYTHONPATH=src "$PYTHON_BIN" scripts/restore_dbw_bronze_release.py \
                        --release-id "$DBW_RELEASE_ID" --data-root "$DBW_DATA_ROOT" \
                        >> "$LOG_DIR/dbw_silver_gold_dbt.log" 2>&1; then
                        echo "[$TIMESTAMP] Verified DBW release restore failed; downstream models remain pending. Check $LOG_DIR/dbw_silver_gold_dbt.log" >> "$SUPERVISOR_LOG"
                    elif ZOHELO_DATA_ROOT="$DBW_DATA_ROOT" \
                        ZOHELO_DBW_BRONZE_RELEASE_ID="$DBW_RELEASE_ID" \
                        ZOHELO_DUCKDB_PATH="$DBW_DUCKDB_PATH" \
                        PYTHONPATH=src "$DBT_BIN" build --profiles-dir . \
                        --select +stg_dbw_observations +stg_dbw_indicators \
                        +stg_dbw_metadata +stg_dbw_dictionaries +dim_dbw_indicator \
                        +fact_dbw_observations +mart_dbw_coverage \
                        --vars '{"enable_gus_dbw": true}' \
                        >> "$LOG_DIR/dbw_silver_gold_dbt.log" 2>&1; then
                        DBW_BRONZE_TRIGGERED_DOWNSTREAM=1
                        echo "[$TIMESTAMP] DBW release $DBW_RELEASE_ID restored and modeled through Gold." >> "$SUPERVISOR_LOG"
                    else
                        echo "[$TIMESTAMP] DBW dbt build failed; downstream models remain pending. Check $LOG_DIR/dbw_silver_gold_dbt.log" >> "$SUPERVISOR_LOG"
                    fi
                fi
            fi
        fi
    fi

    # Check BDL Web Landing process
    BDL_PID="$(pgrep -f "src/bdl_web_adaptive.py" | head -n 1 || true)"
    if [ -n "$BDL_PID" ]; then
        echo "[$TIMESTAMP] BDL Web extractor running (PID $BDL_PID)." >> "$SUPERVISOR_LOG"
    else
        if [ "$BDL_LANDING_TRIGGERED_DOWNSTREAM" -eq 0 ]; then
            SUMMARY_FILE="$LOG_DIR/bdl-web-bulk/bootstrap-summary.json"
            if [ -f "$SUMMARY_FILE" ] && grep -q '"load_complete": true' "$SUMMARY_FILE" 2>/dev/null; then
                echo "[$TIMESTAMP] BDL Web Landing reached 100% completion! Launching decoupled BDL Bronze loader..." >> "$SUPERVISOR_LOG"
                BDL_LANDING_TRIGGERED_DOWNSTREAM=1
                
                # Launch BDL Bronze Loader in background
                PYTHONPATH=src PYTHONUNBUFFERED=1 "$REPO_ROOT/.venv/bin/python" src/bdl_bronze_loader.py --workspace "$LOG_DIR/bdl-bronze" --allow-codespace >> "$LOG_DIR/bdl-bronze/bronze_loader.log" 2>&1 &
            fi
        fi
    fi

    sleep 60
done
