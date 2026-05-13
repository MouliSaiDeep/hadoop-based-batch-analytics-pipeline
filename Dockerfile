FROM apache/airflow:2.8.0

USER root
# Install Java and procps (needed for Spark)
RUN apt-get update && \
    apt-get install -y default-jre-headless procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow
# Install PySpark and Apache Spark provider globally in airflow's virtualenv
RUN pip install --no-cache-dir apache-airflow-providers-apache-spark pyspark==3.5.0
