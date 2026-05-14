"""
Synchronisation PostgreSQL depuis Spark (JDBC + psycopg2 côté driver).
Variables : POSTGRES_HOST, POSTGRES_PORT (5432), POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any, Optional, Sequence, Tuple

logger = logging.getLogger(__name__)


def postgres_enabled() -> bool:
    return bool(os.getenv("POSTGRES_HOST", "").strip())


def jdbc_url() -> str:
    host = os.environ["POSTGRES_HOST"]
    port = int(os.getenv("POSTGRES_PORT", "5432"))
    db = os.getenv("POSTGRES_DB", "recommendation")
    return f"jdbc:postgresql://{host}:{port}/{db}"


def jdbc_properties() -> dict:
    return {
        "user": os.getenv("POSTGRES_USER", "bigdata"),
        "password": os.getenv("POSTGRES_PASSWORD", "bigdata"),
        "driver": "org.postgresql.Driver",
    }


def psycopg2_connect():
    import psycopg2

    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "recommendation"),
        user=os.getenv("POSTGRES_USER", "bigdata"),
        password=os.getenv("POSTGRES_PASSWORD", "bigdata"),
    )


def write_users_and_products(spark, filtered_df, jdbc_url_s: str, props: dict) -> None:
    """Écrase users et products à partir du jeu filtré (même périmètre que l'entraînement ALS)."""
    from pyspark.sql import functions as F

    users_out = (
        filtered_df.groupBy("UserId")
        .agg(
            F.count("*").cast("long").alias("reviews_count"),
            F.avg("Score").alias("avg_score"),
        )
        .select(
            F.col("UserId").alias("user_id"),
            F.col("reviews_count"),
            F.col("avg_score"),
        )
    )

    pname = F.substring(
        F.trim(F.coalesce(F.col("Summary"), F.lit(""))),
        1,
        2000,
    )
    products_out = (
        filtered_df.groupBy("ProductId")
        .agg(
            F.max(pname).alias("product_name"),
            F.lit("amazon_reviews").alias("category"),
            F.avg("Score").alias("avg_rating"),
        )
        .select(
            F.col("ProductId").alias("product_id"),
            F.col("product_name"),
            F.col("category"),
            F.col("avg_rating"),
        )
    )

    logger.info("Écriture JDBC table users …")
    users_out.write.jdbc(jdbc_url_s, "users", "overwrite", props)
    logger.info("Écriture JDBC table products …")
    products_out.write.jdbc(jdbc_url_s, "products", "overwrite", props)


def insert_model_metrics(rmse: float, rank: int, max_iter: int) -> None:
    import psycopg2

    with psycopg2_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO model_metrics (rmse, rank, max_iter)
                VALUES (%s, %s, %s)
                """,
                (float(rmse), int(rank), int(max_iter)),
            )
        conn.commit()


def insert_stream_events(rows: Sequence[Tuple[str, str, Optional[float], Optional[datetime]]]) -> None:
    if not rows:
        return
    import psycopg2
    from psycopg2.extras import execute_batch

    sql = """
        INSERT INTO stream_events (user_id, product_id, score, event_time)
        VALUES (%s, %s, %s, %s)
    """
    with psycopg2_connect() as conn:
        with conn.cursor() as cur:
            execute_batch(cur, sql, list(rows), page_size=500)
        conn.commit()


def insert_recommendations(rows: Sequence[Tuple[str, str, float]]) -> None:
    if not rows:
        return
    import psycopg2
    from psycopg2.extras import execute_batch

    sql = """
        INSERT INTO recommendations (user_id, product_id, predicted_rating)
        VALUES (%s, %s, %s)
    """
    with psycopg2_connect() as conn:
        with conn.cursor() as cur:
            execute_batch(cur, sql, list(rows), page_size=500)
        conn.commit()


def kafka_time_to_dt(t: Any) -> Optional[datetime]:
    if t is None:
        return None
    try:
        sec = int(t)
        if sec <= 0:
            return None
        return datetime.fromtimestamp(sec, tz=timezone.utc)
    except (TypeError, ValueError, OSError):
        return None
