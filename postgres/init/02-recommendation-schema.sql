-- Schéma applicatif principal (recommandations, catalogue, utilisateurs, streaming, métriques)

DROP TABLE IF EXISTS recommendations CASCADE;
DROP TABLE IF EXISTS stream_events CASCADE;
DROP TABLE IF EXISTS model_metrics CASCADE;
DROP TABLE IF EXISTS products CASCADE;
DROP TABLE IF EXISTS users CASCADE;

-- Anciennes tables (migration depuis versions précédentes)
DROP TABLE IF EXISTS recommendation_requests CASCADE;
DROP TABLE IF EXISTS pipeline_runs CASCADE;

-- 1. Recommandations ALS (API + streaming)
CREATE TABLE recommendations (
    id                 BIGSERIAL PRIMARY KEY,
    user_id            TEXT        NOT NULL,
    product_id         TEXT        NOT NULL,
    predicted_rating   DOUBLE PRECISION NOT NULL,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_recommendations_user_id ON recommendations (user_id);
CREATE INDEX IF NOT EXISTS ix_recommendations_created_at ON recommendations (created_at DESC);

-- 2. Produits (agrégés depuis les reviews)
CREATE TABLE products (
    product_id   TEXT PRIMARY KEY,
    product_name TEXT,
    category     TEXT,
    avg_rating   DOUBLE PRECISION NOT NULL
);

-- 3. Utilisateurs (statistiques reviews)
CREATE TABLE users (
    user_id        TEXT PRIMARY KEY,
    reviews_count  BIGINT NOT NULL,
    avg_score      DOUBLE PRECISION NOT NULL
);

-- 4. Événements Kafka consommés en streaming
CREATE TABLE stream_events (
    event_id    BIGSERIAL PRIMARY KEY,
    user_id     TEXT,
    product_id  TEXT,
    score       DOUBLE PRECISION,
    event_time  TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS ix_stream_events_event_time ON stream_events (event_time DESC);

-- 5. Performances du modèle ALS
CREATE TABLE model_metrics (
    id          BIGSERIAL PRIMARY KEY,
    rmse        DOUBLE PRECISION NOT NULL,
    rank        INT NOT NULL,
    max_iter    INT NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
