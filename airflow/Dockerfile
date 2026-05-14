# Copy spark-submit from the already-pulled Spark image — avoids a 300 MB wget
FROM apache/spark:3.5.1 AS spark-dist

FROM apache/airflow:2.9.1-python3.11

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    curl \
    wget \
    procps \
    postgresql-client \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

COPY --from=spark-dist /opt/spark /opt/spark

ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PYSPARK_PYTHON=python3

USER airflow

RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark==4.8.1 \
    numpy==1.26.4 \
    kafka-python==2.0.2

WORKDIR /opt/airflow

HEALTHCHECK --interval=30s --timeout=10s --start-period=90s --retries=3 \
    CMD curl -f http://localhost:8080/health || exit 1
