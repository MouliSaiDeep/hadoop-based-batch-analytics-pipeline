from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id='tower_utilization_heatmap_dag',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:
    
    run_spark_job = BashOperator(
        task_id='submit_tower_heatmap',
        bash_command="""
        export PYSPARK_PYTHON=/opt/bitnami/python/bin/python3
        export PYSPARK_DRIVER_PYTHON=/usr/bin/python3
        spark-submit --master spark://spark-master:7077 /jobs/tower_heatmap.py '{{ dag_run.conf.get("run_id") }}' /data/cdr_data.csv
        """
    )
