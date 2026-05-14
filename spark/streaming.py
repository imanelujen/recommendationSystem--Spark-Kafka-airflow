"""
Spark Structured Streaming — consomme Kafka et génère des recommandations en temps réel.
Charge le modèle ALS pré-entraîné et prédit pour chaque micro-batch.
Persistance PostgreSQL : stream_events (Kafka), recommendations (ALS).
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, FloatType, LongType
from pyspark.ml.recommendation import ALSModel
from pyspark.ml import PipelineModel
import logging

from pg_sync import (
    insert_recommendations,
    insert_stream_events,
    kafka_time_to_dt,
    postgres_enabled,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_SERVERS = "kafka:9092"
KAFKA_TOPIC = "reviews"
MODEL_PATH = "/opt/spark-apps/als_model"
INDEXER_PATH = "/opt/spark-apps/indexer_model"
OUTPUT_PATH = "/opt/spark-apps/recommendations"
CHECKPOINT_DIR = "/opt/spark-apps/checkpoints"

MESSAGE_SCHEMA = StructType(
    [
        StructField("UserId", StringType(), True),
        StructField("ProductId", StringType(), True),
        StructField("Score", FloatType(), True),
        StructField("Time", LongType(), True),
    ]
)


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("ALS-Streaming")
        .master("spark://spark-master:7077")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
        )
        .config("spark.executor.memory", "1536m")
        .config("spark.executor.cores", "1")
        .getOrCreate()
    )


def process_batch(batch_df, batch_id: int, als_model: ALSModel, indexer_pipeline: PipelineModel):
    spark = batch_df.sparkSession
    logger.info("Processing batch %d with %d records", batch_id, batch_df.count())

    if postgres_enabled():
        ev_rows = []
        for r in batch_df.collect():
            try:
                uid, pid = r.UserId, r.ProductId
                if not uid or not pid:
                    continue
                sc = float(r.Score) if r.Score is not None else None
                ev_rows.append((uid, pid, sc, kafka_time_to_dt(r.Time)))
            except Exception as exc:
                logger.debug("Ligne événement ignorée: %s", exc)
        try:
            insert_stream_events(ev_rows)
        except Exception as exc:
            logger.error("insert_stream_events: %s", exc)

    user_indexer = indexer_pipeline.stages[0]
    item_labels = list(indexer_pipeline.stages[1].labels)

    def item_id(idx) -> str:
        ii = int(idx)
        if 0 <= ii < len(item_labels):
            return item_labels[ii]
        return f"product_{ii}"

    schema = StructType([StructField("UserId", StringType(), True)])
    known_labels = list(user_indexer.labels)
    known_users_df = spark.createDataFrame([(u,) for u in known_labels], schema)

    filtered_batch = batch_df.join(known_users_df, on="UserId", how="inner")

    if filtered_batch.isEmpty():
        logger.info("Batch %d contains no known users, skipping", batch_id)
        return

    indexed_df = indexer_pipeline.transform(filtered_batch)
    user_mapping = indexed_df.select("UserId", "userIndex").distinct()
    indexed_users = indexed_df.select(
        F.col("userIndex").cast("int").alias("userId")
    ).distinct()

    recs = als_model.recommendForUserSubset(indexed_users, 10)

    recs_flat = (
        recs.alias("r")
        .join(
            user_mapping.alias("m"),
            F.col("r.userId") == F.col("m.userIndex"),
            "inner",
        )
        .select(F.col("m.UserId"), F.explode("recommendations").alias("rec"))
        .select(
            F.col("m.UserId"),
            F.col("rec.itemId").alias("itemId"),
            F.col("rec.rating").alias("predicted_rating"),
        )
    )

    (
        recs_flat.write.mode("append").json(f"{OUTPUT_PATH}/batch_{batch_id}")
    )

    n = recs_flat.count()
    logger.info("Batch %d — %d recommandations générées", batch_id, n)

    if postgres_enabled():
        rec_rows = []
        for r in recs_flat.collect():
            try:
                rec_rows.append(
                    (r.UserId, item_id(r.itemId), float(r.predicted_rating))
                )
            except Exception as exc:
                logger.debug("Reco ignorée: %s", exc)
        try:
            insert_recommendations(rec_rows)
        except Exception as exc:
            logger.error("insert_recommendations: %s", exc)


if __name__ == "__main__":
    spark = create_spark_session()
    als_model = ALSModel.load(MODEL_PATH)
    indexer_pipeline = PipelineModel.load(INDEXER_PATH)

    kafka_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = (
        kafka_stream.select(
            F.from_json(F.col("value").cast("string"), MESSAGE_SCHEMA).alias("data")
        )
        .select("data.*")
        .filter(F.col("UserId").isNotNull())
    )

    query = (
        parsed.writeStream.foreachBatch(
            lambda df, bid: process_batch(df, bid, als_model, indexer_pipeline)
        )
        .option("checkpointLocation", CHECKPOINT_DIR)
        .trigger(processingTime="30 seconds")
        .start()
    )

    query.awaitTermination()
