from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id='revenue_reconciliation_dag',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:
    
    run_spark_job = BashOperator(
        task_id='submit_revenue_recon',
        bash_command="""
        spark-submit --master spark://spark-master:7077 --conf spark.pyspark.python=/usr/bin/python3 --conf spark.pyspark.driver.python=/usr/bin/python3 /jobs/revenue_recon.py '{{ dag_run.conf.get("run_id") or dag_run.run_id }}' /data/cdr_data.csv
        """
    )
