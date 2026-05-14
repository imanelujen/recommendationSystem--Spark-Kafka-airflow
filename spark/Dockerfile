
FROM apache/spark:3.5.1

# Passer en root pour installer les dépendances système
USER root

# ── Dépendances système ──────────────────────────────────────────────────────
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    wget \
    procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# ── Dépendances Python pour PySpark ─────────────────────────────────────────
RUN pip install --no-cache-dir \
    kafka-python==2.0.2 \
    numpy==1.24.0 \
    pandas==2.0.0 \
    pyarrow==12.0.0 \
    scikit-learn==1.3.0 \
    psycopg2-binary==2.9.9

# ── Télécharger les JARs Kafka + PostgreSQL en une seule couche ─────────────
RUN set -eux; BASE=https://repo1.maven.org/maven2; \
    wget -q "$BASE/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar" \
         -O /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.1.jar && \
    wget -q "$BASE/org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar" \
         -O /opt/spark/jars/kafka-clients-3.4.1.jar && \
    wget -q "$BASE/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar" \
         -O /opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.1.jar && \
    wget -q "$BASE/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar" \
         -O /opt/spark/jars/commons-pool2-2.11.1.jar && \
    wget -q "$BASE/org/lz4/lz4-java/1.8.0/lz4-java-1.8.0.jar" \
         -O /opt/spark/jars/lz4-java-1.8.0.jar && \
    wget -q "$BASE/org/xerial/snappy/snappy-java/1.1.10.5/snappy-java-1.1.10.5.jar" \
         -O /opt/spark/jars/snappy-java-1.1.10.5.jar && \
    wget -q "$BASE/org/postgresql/postgresql/42.7.3/postgresql-42.7.3.jar" \
         -O /opt/spark/jars/postgresql-42.7.3.jar

# ── Créer les répertoires de travail ────────────────────────────────────────
RUN mkdir -p /opt/spark-apps \
             /opt/spark-data \
             /opt/spark-apps/als_model \
             /opt/spark-apps/indexer_model \
             /opt/spark-apps/recommendations \
             /opt/spark-apps/checkpoints \
             /opt/spark/logs

# ── Copier les scripts Spark ─────────────────────────────────────────────────
COPY train_als.py    /opt/spark-apps/train_als.py
COPY streaming.py    /opt/spark-apps/streaming.py
COPY pg_sync.py      /opt/spark-apps/pg_sync.py

# ── Permissions ─────────────────────────────────────────────────────────────
RUN chmod -R 755 /opt/spark-apps /opt/spark/logs && \
    chown -R 1001:1001 /opt/spark-apps /opt/spark-data /opt/spark/logs


# USER 1001  # Reste en root pour éviter les problèmes Hadoop

# ── Variables d'environnement Spark ─────────────────────────────────────────
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# Répertoire de travail par défaut
WORKDIR /opt/spark-apps