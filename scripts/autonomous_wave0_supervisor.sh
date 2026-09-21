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
            if grep -q "1550/1550" "$LOG_DIR/dbw-bronze/bronze_loader.log" 2>/dev/null || grep -q "Bronze transformation complete" "$LOG_DIR/dbw-bronze/bronze_loader.log" 2>/dev/null; then
                echo "[$TIMESTAMP] DBW Bronze reached 100% completion! Launching DBW Silver and Gold dbt models..." >> "$SUPERVISOR_LOG"
                DBW_BRONZE_TRIGGERED_DOWNSTREAM=1
                
                # Execute DBW Silver & Gold dbt models
                PYTHONPATH=src dbt build --profiles-dir . --select +stg_dbw_observations +stg_dbw_indicators +stg_dbw_metadata +stg_dbw_dictionaries +dim_dbw_indicator +fact_dbw_observations +mart_dbw_coverage --vars '{"enable_gus_dbw": true}' >> "$LOG_DIR/dbw_silver_gold_dbt.log" 2>&1 || {
                    echo "[$TIMESTAMP] DBW dbt build failed. Check $LOG_DIR/dbw_silver_gold_dbt.log" >> "$SUPERVISOR_LOG"
                }
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
