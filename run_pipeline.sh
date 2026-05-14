#!/bin/bash
set -e

if [ "$#" -ne 1 ]; then
    echo "Usage: ./run_pipeline.sh <logical_query_name>"
    echo "Supported queries: top_callers, tower_heatmap, anomalous_calls, revenue_recon"
    exit 1
fi

QUERY=$1
DAG_ID=""

case $QUERY in
    "top_callers")
        DAG_ID="top_callers_by_spend_dag"
        ;;
    "tower_heatmap")
        DAG_ID="tower_utilization_heatmap_dag"
        ;;
    "anomalous_calls")
        DAG_ID="anomalous_call_detection_dag"
        ;;
    "revenue_recon")
        DAG_ID="revenue_reconciliation_dag"
        ;;
    *)
        echo "Error: Unknown query '$QUERY'"
        echo "Supported queries: top_callers, tower_heatmap, anomalous_calls, revenue_recon"
        exit 1
        ;;
esac

RUN_ID=$(date +"%Y%m%d_%H%M%S")

echo "Triggering Airflow DAG: $DAG_ID with run_id: $RUN_ID"

docker compose exec -T airflow airflow dags trigger -r "$RUN_ID" --conf "{\"run_id\":\"$RUN_ID\"}" "$DAG_ID"

echo "Waiting for DAG $DAG_ID to complete..."
TIMEOUT=300
ELAPSED=0
INTERVAL=5

while [ $ELAPSED -lt $TIMEOUT ]; do
    # Fetch DAG run state dynamically and robustly via JSON API
    STATE=$(docker compose exec -T airflow python3 -c "import subprocess, json; runs = json.loads(subprocess.check_output(['airflow', 'dags', 'list-runs', '-d', '$DAG_ID', '-o', 'json']).decode('utf-8')); matching = [r['state'] for r in runs if r['run_id'] == '$RUN_ID']; print(matching[0] if matching else '')" 2>/dev/null || true)
    
    if [ "$STATE" == "success" ]; then
        echo "DAG completed successfully."
        exit 0
    elif [ "$STATE" == "failed" ]; then
        echo "DAG failed."
        exit 1
    fi
    
    sleep $INTERVAL
    ELAPSED=$((ELAPSED + INTERVAL))
done

echo "Error: DAG run timed out after ${TIMEOUT} seconds."
exit 1
