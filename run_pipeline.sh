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

echo "DAG triggered successfully."
