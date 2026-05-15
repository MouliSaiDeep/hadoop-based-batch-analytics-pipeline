FROM apache/airflow:2.8.0

USER root
# Install Java, procps, python3-pip, curl, and system-wide PySpark 3.5.0 for Python 3.11
RUN apt-get update && \
    apt-get install -y default-jre-headless procps python3-pip curl && \
    env PIP_USER=false /usr/bin/python3.11 -m pip install --ignore-installed --break-system-packages pyspark==3.5.0 && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install Apache Spark 3.5.0
ENV SPARK_VERSION=3.5.0
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

RUN curl -sL https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz | tar -xz -C /opt && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop3 $SPARK_HOME && \
    chown -R airflow:root $SPARK_HOME

USER airflow
# Install PySpark and Apache Spark provider globally in airflow's virtualenv
RUN pip install --no-cache-dir apache-airflow-providers-apache-spark pyspark==3.5.0

