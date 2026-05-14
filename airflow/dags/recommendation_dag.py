"""
DAG Airflow — pipeline réel : Kafka (producteur) → entraînement ALS Spark → streaming (fenêtre) → santé API.
Les métriques modèle et dimensions sont écrites par train_als ; le streaming et l'API alimentent PostgreSQL.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule

default_args = {
    "owner": "data-team",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="recommendation_pipeline",
    description="Kafka + Spark ALS + streaming + API + PostgreSQL",
    schedule="0 2 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["bigdata", "ml", "kafka", "spark", "postgres"],
) as dag:

    check_kafka = BashOperator(
        task_id="check_kafka_health",
        bash_command="timeout 5 bash -c 'echo > /dev/tcp/kafka/9092' && echo 'Kafka OK'",
    )

    run_kafka_producer = BashOperator(
        task_id="run_kafka_producer",
        bash_command=(
            "export KAFKA_BOOTSTRAP_SERVERS=kafka:9092 KAFKA_TOPIC=reviews "
            "DATA_PATH=/opt/spark-data/Reviews.csv DELAY_SECONDS=0 && "
            "timeout 300 python3 /opt/kafka-apps/producer.py"
        ),
        execution_timeout=timedelta(minutes=7),
    )

    train_als_model = BashOperator(
        task_id="train_als_model",
        bash_command=(
            "set -euo pipefail\n"
            "spark-submit "
            "--master spark://spark-master:7077 "
            "--deploy-mode client "
            "--driver-memory 2g "
            "--executor-memory 2g "
            "/opt/spark-apps/train_als.py\n"
        ),
        execution_timeout=timedelta(hours=2),
    )

    run_streaming_window = BashOperator(
        task_id="run_spark_streaming_window",
        bash_command=(
            "set +e\n"
            "timeout 180 spark-submit "
            "--master spark://spark-master:7077 "
            "--deploy-mode client "
            "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 "
            "--driver-memory 1536m "
            "--executor-memory 1536m "
            "--executor-cores 1 "
            "/opt/spark-apps/streaming.py\n"
            "code=$?\n"
            "if [ \"$code\" -eq 124 ]; then echo 'Streaming arrêté après timeout (OK)'; exit 0; fi\n"
            "exit \"$code\"\n"
        ),
        execution_timeout=timedelta(minutes=10),
    )

    check_api = BashOperator(
        task_id="check_api_health",
        bash_command=(
            "for i in $(seq 1 30); do "
            "curl -sf http://api:8000/health | grep -q '\"model_loaded\": true' && exit 0; "
            "sleep 5; done; echo 'API ou modèle non prêt'; exit 1"
        ),
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    notify_success = BashOperator(
        task_id="notify_success",
        bash_command="echo \"Pipeline terminé avec succès à $(date -u)\"",
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    (
        check_kafka
        >> run_kafka_producer
        >> train_als_model
        >> run_streaming_window
        >> check_api
        >> notify_success
    )
