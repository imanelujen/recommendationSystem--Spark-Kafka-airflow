"""
Entraînement du modèle ALS avec Spark MLlib.
Split: 80% train / 10% validation / 10% test
Métrique d'évaluation: RMSE
Persistance PostgreSQL : users, products, model_metrics (si POSTGRES_HOST défini).
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
import logging
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MODEL_PATH = "/opt/spark-apps/als_model"
DATA_PATH = "/opt/spark-data/Reviews.csv"


def create_spark_session() -> SparkSession:
    spark = (
        SparkSession.builder.appName("ALS-Training")
        .master("spark://spark-master:7077")
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "2g")
        .getOrCreate()
    )
    spark.sparkContext.setCheckpointDir("/opt/spark-apps/checkpoints")
    return spark


def read_reviews_raw(spark: SparkSession):
    return (
        spark.read.csv(DATA_PATH, header=True, inferSchema=True)
        .select(
            "UserId",
            "ProductId",
            "Score",
            F.col("Summary").cast("string").alias("Summary"),
        )
        .dropna(subset=["UserId", "ProductId", "Score"])
        .filter(~F.isnan(F.col("Score")))
        .dropDuplicates(["UserId", "ProductId"])
    )


def filter_active_users_items(df):
    """Même filtre que précédemment : users et produits avec au moins 5 interactions."""
    user_counts = df.groupBy("UserId").count().filter(F.col("count") >= 5)
    item_counts = df.groupBy("ProductId").count().filter(F.col("count") >= 5)
    return (
        df.join(user_counts.select("UserId"), "UserId")
        .join(item_counts.select("ProductId"), "ProductId")
    )


def build_indexed_ratings(spark: SparkSession, filtered_df):
    from pyspark.ml.feature import StringIndexer
    from pyspark.ml import Pipeline

    base = filtered_df.select("UserId", "ProductId", "Score")
    user_indexer = StringIndexer(inputCol="UserId", outputCol="userIndex")
    item_indexer = StringIndexer(inputCol="ProductId", outputCol="itemIndex")
    pipeline = Pipeline(stages=[user_indexer, item_indexer])

    model = pipeline.fit(base)
    df_index = model.transform(base)

    model.write().overwrite().save("/opt/spark-apps/indexer_model")

    df_final = (
        df_index.select(
            F.col("userIndex").cast("int").alias("userId"),
            F.col("itemIndex").cast("int").alias("itemId"),
            F.col("Score").cast("float").alias("rating"),
        )
        .dropna()
        .filter(~(F.col("rating") != F.col("rating")))
    )

    df_final = df_final.cache()
    logger.info("Données finales en cache, nombre de lignes: %d", df_final.count())
    return df_final


def train_als(df):
    train, val, test = df.randomSplit([0.8, 0.1, 0.1], seed=42)
    logger.info("Split — train: %d, val: %d, test: %d", train.count(), val.count(), test.count())

    als = ALS(
        userCol="userId",
        itemCol="itemId",
        ratingCol="rating",
        coldStartStrategy="drop",
        nonnegative=True,
        checkpointInterval=10,
    )

    param_grid = (
        ParamGridBuilder()
        .addGrid(als.rank, [10, 20])
        .addGrid(als.maxIter, [10, 20])
        .addGrid(als.regParam, [0.01, 0.1])
        .build()
    )

    evaluator = RegressionEvaluator(
        metricName="rmse",
        labelCol="rating",
        predictionCol="prediction",
    )

    cv = CrossValidator(
        estimator=als,
        estimatorParamMaps=param_grid,
        evaluator=evaluator,
        numFolds=3,
        parallelism=2,
    )

    logger.info("Début de l'entraînement (CrossValidation)...")
    cv_model = cv.fit(train)
    best_model = cv_model.bestModel

    val_preds = best_model.transform(val).dropna()
    val_rmse = evaluator.evaluate(val_preds)
    logger.info("RMSE (validation): %.4f", val_rmse)

    test_preds = best_model.transform(test).dropna()
    test_rmse = evaluator.evaluate(test_preds)
    logger.info("RMSE (test): %.4f", test_rmse)

    best_model.write().overwrite().save(MODEL_PATH)
    logger.info("Modèle sauvegardé dans %s", MODEL_PATH)

    return best_model, val_rmse, test_rmse


def _als_max_iter(model) -> int:
    try:
        return int(model._java_obj.getMaxIter())
    except Exception:
        pass
    try:
        return int(model._java_obj.parent().getMaxIter())
    except Exception:
        pass
    return -1


if __name__ == "__main__":
    from pg_sync import (
        insert_model_metrics,
        jdbc_properties,
        jdbc_url,
        postgres_enabled,
        write_users_and_products,
    )

    spark = create_spark_session()
    raw = read_reviews_raw(spark)
    filtered = filter_active_users_items(raw)

    if postgres_enabled():
        try:
            write_users_and_products(spark, filtered, jdbc_url(), jdbc_properties())
        except Exception as exc:
            logger.error("Échec écriture users/products JDBC: %s", exc)
            raise

    df_final = build_indexed_ratings(spark, filtered)
    best_model, _val_rmse, test_rmse = train_als(df_final)

    if postgres_enabled():
        try:
            mi = _als_max_iter(best_model)
            if mi < 0:
                mi = 10
            insert_model_metrics(
                rmse=test_rmse,
                rank=int(best_model.rank),
                max_iter=mi,
            )
        except Exception as exc:
            logger.error("Échec insert model_metrics: %s", exc)
            raise

    spark.stop()
