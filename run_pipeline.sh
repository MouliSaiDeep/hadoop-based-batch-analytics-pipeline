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
        JOB_NAME="top_callers_by_spend"
        ;;
    "tower_heatmap")
        DAG_ID="tower_utilization_heatmap_dag"
        JOB_NAME="tower_utilization_heatmap"
        ;;
    "anomalous_calls")
        DAG_ID="anomalous_call_detection_dag"
        JOB_NAME="anomalous_call_detection"
        ;;
    "revenue_recon")
        DAG_ID="revenue_reconciliation_dag"
        JOB_NAME="revenue_reconciliation"
        ;;
    *)
        echo "Error: Unknown query '$QUERY'"
        echo "Supported queries: top_callers, tower_heatmap, anomalous_calls, revenue_recon"
        exit 1
esac

# Verify that Docker Compose is running and containers are active
if ! docker compose ps --filter "status=running" | grep -q "airflow"; then
    echo "Error: Docker containers do not appear to be running or are unhealthy. Please run 'docker compose up -d' first."
    exit 1
fi

# Pre-create the host-side output directory and subdirectories to ensure correct bind mount access
mkdir -p output/top_callers_by_spend
mkdir -p output/tower_utilization_heatmap
mkdir -p output/anomalous_call_detection
mkdir -p output/revenue_reconciliation

# Wait for HDFS NameNode to exit safemode safely with a 180-second timeout
echo "Waiting for HDFS NameNode to exit safemode..."
timeout 180 docker compose exec -T namenode hdfs dfsadmin -safemode wait || { echo "Error: NameNode safemode wait timed out"; exit 1; }

# Ensure HDFS staging output directory exists with open permissions
echo "Ensuring HDFS directory tree..."
docker compose exec -T namenode hdfs dfs -mkdir -p /tmp/output
docker compose exec -T namenode hdfs dfs -chmod -R 777 /tmp

RUN_ID=$(date +"%Y%m%d_%H%M%S")

echo "Triggering Airflow DAG: $DAG_ID with run_id: $RUN_ID"

docker compose exec -T airflow airflow dags trigger -r "$RUN_ID" --conf "{\"run_id\":\"$RUN_ID\"}" "$DAG_ID"

echo "Waiting for DAG $DAG_ID to complete..."
TIMEOUT=300
ELAPSED=0
INTERVAL=5

while [ $ELAPSED -lt $TIMEOUT ]; do
    # Fetch DAG run state dynamically and robustly via JSON API
    STATE=$(docker compose exec -T airflow python3 -c "
import subprocess, json, sys
try:
    out = subprocess.check_output(['airflow', 'dags', 'list-runs', '-d', '$DAG_ID', '--output', 'json'], stderr=subprocess.DEVNULL).decode('utf-8').strip()
    if out:
        runs = json.loads(out)
        matching = [r['state'] for r in runs if r.get('dag_run_id') == '$RUN_ID' or r.get('run_id') == '$RUN_ID']
        if matching:
            print(matching[0])
            sys.exit(0)
except Exception as e:
    pass
print('')
" 2>/dev/null || true)
    
    if [ "$STATE" == "success" ]; then
        echo "DAG completed successfully."
        
        # Verify the presence of the local host manifest file
        MANIFEST_PATH="output/${JOB_NAME}/${RUN_ID}/_MANIFEST.json"
        echo "Verifying manifest file presence at $MANIFEST_PATH..."
        if [ ! -f "$MANIFEST_PATH" ]; then
            echo "Error: Manifest file not found at $MANIFEST_PATH"
            exit 1
        fi
        
        # Parse and check manifest status is "SUCCESS"
        STATUS=$(python3 -c "
import json
try:
    with open('$MANIFEST_PATH', 'r') as f:
        data = json.load(f)
        print(data.get('status', ''))
except Exception:
    print('')
")
        if [ "$STATUS" != "SUCCESS" ]; then
            echo "Error: Manifest status is '$STATUS', expected 'SUCCESS'"
            exit 1
        fi
        
        echo "Manifest validation passed successfully!"
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
