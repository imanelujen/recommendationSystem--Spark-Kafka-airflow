"""
API FastAPI — ALS PySpark recommendations + analytics + monitoring endpoints
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Tuple
import logging
import os
import threading
import time
import socket

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Système de Recommandation — Amazon Reviews",
    description="API REST — recommandations temps réel avec ALS Spark MLlib",
    version="2.0.0",
)

@app.get("/", include_in_schema=False)
def root():
    from fastapi.responses import RedirectResponse
    return RedirectResponse(url="/docs")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

MODEL_PATH   = os.getenv("MODEL_PATH",   "/app/als_model")
INDEXER_PATH = os.getenv("INDEXER_PATH", "/app/indexer_model")
TOP_N        = int(os.getenv("TOP_N", "10"))
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()

_spark       = None
_als_model   = None
_indexer     = None
_user_map    = {}
_item_map    = {}
_lock        = threading.Lock()
_model_ready = False


# ── DB helpers ────────────────────────────────────────────────

def _db_conn():
    import psycopg2
    return psycopg2.connect(DATABASE_URL)


def _create_api_logs_table():
    if not DATABASE_URL:
        return
    try:
        with _db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS api_logs (
                        id         BIGSERIAL PRIMARY KEY,
                        user_id    TEXT,
                        top_n      INTEGER,
                        latency_ms FLOAT,
                        status_code INTEGER,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT now()
                    );
                    CREATE INDEX IF NOT EXISTS ix_api_logs_created_at
                    ON api_logs (created_at DESC);
                """)
            conn.commit()
    except Exception as exc:
        logger.warning("Create api_logs: %s", exc)


def _log_api_call(user_id: str, top_n: int, latency_ms: float, status_code: int):
    if not DATABASE_URL:
        return
    try:
        with _db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO api_logs (user_id, top_n, latency_ms, status_code) VALUES (%s, %s, %s, %s)",
                    (user_id, top_n, latency_ms, status_code),
                )
            conn.commit()
    except Exception as exc:
        logger.debug("Log API call: %s", exc)


def _persist_recommendations(user_id: str, pairs: List[Tuple[str, float]]) -> None:
    if not DATABASE_URL or not pairs:
        return
    try:
        from psycopg2.extras import execute_batch
        with _db_conn() as conn:
            with conn.cursor() as cur:
                execute_batch(
                    cur,
                    "INSERT INTO recommendations (user_id, product_id, predicted_rating) VALUES (%s, %s, %s)",
                    [(user_id, pid, float(r)) for pid, r in pairs],
                    page_size=100,
                )
            conn.commit()
    except Exception as exc:
        logger.warning("Persistance recommendations: %s", exc)


# ── Pydantic models ───────────────────────────────────────────

class RecommendationResponse(BaseModel):
    user_id:         str
    recommendations: List[str]
    count:           int
    model_used:      str = "ALS"
    latency_ms:      float = 0.0
    cached:          bool = False

    model_config = {'protected_namespaces': ()}

class HealthResponse(BaseModel):
    status:        str
    version:       str
    model_loaded:  bool
    users_indexed: int
    postgres:      str = "unknown"

    model_config = {'protected_namespaces': ()}


# ── Spark model init ──────────────────────────────────────────

def init_spark_model():
    global _spark, _als_model, _indexer, _user_map, _item_map, _model_ready
    try:
        from pyspark.sql import SparkSession
        from pyspark.ml.recommendation import ALSModel
        from pyspark.ml import PipelineModel

        logger.info("Initialisation de la session Spark locale...")
        _spark = (
            SparkSession.builder
            .appName("RecommendationAPI")
            .master("local[2]")
            .config("spark.driver.memory", "1g")
            .config("spark.ui.enabled", "false")
            .config("spark.log.level", "ERROR")
            .getOrCreate()
        )
        _spark.sparkContext.setLogLevel("ERROR")

        logger.info("Chargement ALS depuis %s", MODEL_PATH)
        _als_model = ALSModel.load(MODEL_PATH)

        logger.info("Chargement indexer depuis %s", INDEXER_PATH)
        _indexer = PipelineModel.load(INDEXER_PATH)

        user_labels = _indexer.stages[0].labels
        item_labels = _indexer.stages[1].labels
        _user_map = {label: idx for idx, label in enumerate(user_labels)}
        _item_map = {idx: label for idx, label in enumerate(item_labels)}

        _model_ready = True
        logger.info("Modèle prêt — %d users, %d items", len(_user_map), len(_item_map))
    except Exception as e:
        logger.error("Erreur chargement modèle: %s", e)
        _model_ready = False


@app.on_event("startup")
async def startup_event():
    _create_api_logs_table()
    threading.Thread(target=init_spark_model, daemon=True).start()


# ── Core endpoints ────────────────────────────────────────────

@app.get("/health", response_model=HealthResponse)
def health_check():
    pg_status = "unknown"
    if DATABASE_URL:
        try:
            with _db_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1")
            pg_status = "ok"
        except Exception:
            pg_status = "error"
    return {
        "status":        "ok",
        "version":       "2.0.0",
        "model_loaded":  _model_ready,
        "users_indexed": len(_user_map),
        "postgres":      pg_status,
    }


@app.get("/stats")
def get_stats():
    latest_rmse = None
    total_api_calls = 0
    avg_latency_ms = 0.0
    if DATABASE_URL:
        try:
            with _db_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT rmse FROM model_metrics ORDER BY created_at DESC LIMIT 1")
                    row = cur.fetchone()
                    if row:
                        latest_rmse = row[0]
                    cur.execute("SELECT COUNT(*), COALESCE(AVG(latency_ms),0) FROM api_logs WHERE status_code=200")
                    row = cur.fetchone()
                    if row:
                        total_api_calls = row[0]
                        avg_latency_ms  = round(float(row[1]), 1)
        except Exception as exc:
            logger.debug("stats query: %s", exc)
    return {
        "model_loaded":    _model_ready,
        "total_users":     len(_user_map),
        "total_products":  len(_item_map),
        "model_path":      MODEL_PATH,
        "postgres_logging": bool(DATABASE_URL),
        "latest_rmse":     latest_rmse,
        "total_api_calls": total_api_calls,
        "avg_latency_ms":  avg_latency_ms,
    }


@app.get(
    "/recommendations/user/{user_id}",
    response_model=RecommendationResponse,
    summary="Top-N recommandations pour un utilisateur",
)
def get_recommendations(user_id: str, top_n: int = TOP_N):
    t0 = time.time()

    if not _model_ready:
        _log_api_call(user_id, top_n, 0, 503)
        raise HTTPException(status_code=503, detail="Modèle en cours de chargement, réessayez dans 30 secondes.")

    if top_n < 1 or top_n > 50:
        raise HTTPException(status_code=422, detail="top_n doit être entre 1 et 50")

    user_index = _user_map.get(user_id)
    if user_index is None:
        _log_api_call(user_id, top_n, round((time.time()-t0)*1000, 1), 404)
        raise HTTPException(status_code=404, detail=f"Utilisateur '{user_id}' non trouvé.")

    with _lock:
        try:
            from pyspark.sql import Row
            user_df = _spark.createDataFrame([Row(userId=int(user_index))], schema="userId INT")
            recs = _als_model.recommendForUserSubset(user_df, top_n)
            rows = recs.collect()
            if not rows:
                _log_api_call(user_id, top_n, round((time.time()-t0)*1000, 1), 404)
                raise HTTPException(status_code=404, detail=f"Aucune recommandation pour '{user_id}'.")
            pairs = [(_item_map.get(rec.itemId, f"product_{rec.itemId}"), float(rec.rating))
                     for rec in rows[0].recommendations]
        except HTTPException:
            raise
        except Exception as e:
            latency = round((time.time()-t0)*1000, 1)
            _log_api_call(user_id, top_n, latency, 500)
            raise HTTPException(status_code=500, detail=str(e))

    latency = round((time.time()-t0)*1000, 1)
    _log_api_call(user_id, top_n, latency, 200)
    _persist_recommendations(user_id, pairs)

    return RecommendationResponse(
        user_id=user_id,
        recommendations=[p for p, _ in pairs],
        count=len(pairs),
        latency_ms=latency,
    )


@app.get("/users/sample")
def get_sample_users(n: int = 10):
    return {"sample_users": list(_user_map.keys())[:n], "usage": "/recommendations/user/{user_id}"}


@app.get("/debug/users")
def debug_users(n: int = 20):
    if not _model_ready:
        raise HTTPException(status_code=503, detail="Modèle non chargé")
    return {"total_indexed": len(_user_map), "sample_userids": list(_user_map.keys())[:n]}


@app.get("/debug/check/{user_id}")
def debug_check_user(user_id: str):
    index = _user_map.get(user_id)
    return {"user_id": user_id, "found": index is not None, "index": index, "total_users_in_model": len(_user_map)}


# ── Analytics ─────────────────────────────────────────────────

@app.get("/analytics/top-products")
def top_products(limit: int = 10):
    if not DATABASE_URL:
        return {"data": []}
    try:
        with _db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT product_id, COUNT(*) as cnt FROM recommendations GROUP BY product_id ORDER BY cnt DESC LIMIT %s",
                    (limit,),
                )
                rows = cur.fetchall()
        return {"data": [{"product_id": r[0], "count": r[1]} for r in rows]}
    except Exception as exc:
        logger.warning("top_products: %s", exc)
        return {"data": []}


@app.get("/analytics/requests-per-hour")
def requests_per_hour():
    if not DATABASE_URL:
        return {"data": []}
    try:
        with _db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT date_trunc('hour', created_at) AS hour,
                           COUNT(*) AS requests,
                           COALESCE(AVG(latency_ms), 0) AS avg_latency
                    FROM api_logs
                    WHERE created_at >= NOW() - INTERVAL '24 hours'
                    GROUP BY hour ORDER BY hour
                """)
                rows = cur.fetchall()
        return {"data": [{"hour": r[0].isoformat(), "requests": r[1], "avg_latency": round(float(r[2]), 1)} for r in rows]}
    except Exception as exc:
        logger.warning("requests_per_hour: %s", exc)
        return {"data": []}


@app.get("/analytics/model-metrics")
def model_metrics_endpoint():
    if not DATABASE_URL:
        return {"data": []}
    try:
        with _db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id, rmse, rank, max_iter, created_at FROM model_metrics ORDER BY created_at DESC LIMIT 10")
                rows = cur.fetchall()
        return {"data": [
            {
                "model_version": f"v{r[0]}",
                "rmse_test":  r[1],
                "rmse_val":   r[1],
                "train_size": len(_user_map),
                "trained_at": r[4].isoformat() if r[4] else None,
                "rank":       r[2],
                "max_iter":   r[3],
            }
            for r in rows
        ]}
    except Exception as exc:
        logger.warning("model_metrics: %s", exc)
        return {"data": []}


# ── Monitoring ────────────────────────────────────────────────

def _tcp_up(host: str, port: int, timeout: float = 2.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except Exception:
        return False


@app.get("/monitoring/services")
def monitoring_services():
    return {"services": [
        {"name": "API FastAPI",   "status": "up",                                          "url": "localhost:8000"},
        {"name": "PostgreSQL",    "status": "up" if _tcp_up("postgres", 5432) else "down", "url": "postgres:5432"},
        {"name": "Spark Master",  "status": "up" if _tcp_up("spark-master", 7077) else "down", "url": "localhost:8080"},
        {"name": "Kafka",         "status": "up" if _tcp_up("kafka", 9092) else "down",    "url": "kafka:9092"},
        {"name": "Airflow",       "status": "up" if _tcp_up("airflow-webserver", 8080) else "down", "url": "localhost:8088"},
    ]}


# ── Streaming ─────────────────────────────────────────────────

@app.get("/streaming/stats")
def streaming_stats():
    if not DATABASE_URL:
        return {"total_events": 0, "events_last_hour": 0}
    try:
        with _db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT COUNT(*) FROM stream_events")
                total = cur.fetchone()[0]
                cur.execute("SELECT COUNT(*) FROM stream_events WHERE event_time >= NOW() - INTERVAL '1 hour'")
                last_hour = cur.fetchone()[0]
        return {"total_events": total, "events_last_hour": last_hour}
    except Exception as exc:
        logger.warning("streaming_stats: %s", exc)
        return {"total_events": 0, "events_last_hour": 0}


@app.get("/streaming/live")
def streaming_live(limit: int = 20):
    if not DATABASE_URL:
        return {"events": []}
    try:
        with _db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT user_id, product_id, score, event_time FROM stream_events ORDER BY event_id DESC LIMIT %s",
                    (limit,),
                )
                rows = cur.fetchall()
        return {"events": [
            {"user_id": r[0], "product_id": r[1], "score": r[2],
             "ingested_at": r[3].isoformat() if r[3] else None}
            for r in rows
        ]}
    except Exception as exc:
        logger.warning("streaming_live: %s", exc)
        return {"events": []}


# ── Logs ──────────────────────────────────────────────────────

@app.get("/logs/api")
def logs_api(limit: int = 50):
    if not DATABASE_URL:
        return {"data": []}
    try:
        with _db_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT user_id, top_n, latency_ms, status_code, created_at FROM api_logs ORDER BY created_at DESC LIMIT %s",
                    (limit,),
                )
                rows = cur.fetchall()
        return {"data": [
            {"user_id": r[0], "top_n": r[1], "latency_ms": r[2], "status_code": r[3],
             "created_at": r[4].isoformat() if r[4] else None}
            for r in rows
        ]}
    except Exception as exc:
        logger.warning("logs_api: %s", exc)
        return {"data": []}
