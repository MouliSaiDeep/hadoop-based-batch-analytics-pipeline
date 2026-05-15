from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id='top_callers_by_spend_dag',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:
    
    run_spark_job = BashOperator(
        task_id='submit_top_callers',
        bash_command="""
        spark-submit --master spark://spark-master:7077 --conf spark.pyspark.python=/usr/bin/python3 --conf spark.pyspark.driver.python=/usr/bin/python3 /jobs/top_callers.py '{{ dag_run.conf.get("run_id") or run_id }}' /data/cdr_data.csv
        """
    )
