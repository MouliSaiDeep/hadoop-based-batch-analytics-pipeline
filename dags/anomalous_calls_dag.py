from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id='anomalous_call_detection_dag',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:
    
    run_spark_job = BashOperator(
        task_id='submit_anomalous_calls',
        bash_command="""
        export PATH=$PATH:~/.local/bin
        spark-submit --master spark://spark-master:7077 /jobs/anomalous_calls.py '{{ dag_run.conf.get("run_id") }}'
        """
    )
