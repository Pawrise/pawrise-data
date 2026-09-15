-- 1. Initialisation des extensions requises
CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS vector;

-- 2. Création de la table des métriques physiologiques anonymisées
CREATE TABLE IF NOT EXISTS dog_metrics (
    recorded_at TIMESTAMPTZ NOT NULL,
    anonymous_collar_id UUID NOT NULL,
    bpm SMALLINT NOT NULL CHECK (bpm > 0),
    respiratory_rate SMALLINT NOT NULL CHECK (respiratory_rate > 0),
    temperature REAL NOT NULL, -- Température en degrés Celsius
    UNIQUE (anonymous_collar_id, recorded_at)
);

-- 3. Transformation en Hypertable (TimescaleDB)
SELECT create_hypertable(
    'dog_metrics', 
    'recorded_at', 
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

-- 4. Index optimisé pour nos services métier (Handoff) et RAG
CREATE INDEX IF NOT EXISTS ix_dog_metrics_collar_time 
ON dog_metrics (anonymous_collar_id, recorded_at DESC);

-- 5. Table pour l'architecture RAG (Résultats / Insights générés)
CREATE TABLE IF NOT EXISTS ai_insights (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    anonymous_collar_id UUID NOT NULL,
    generated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    context_summary TEXT NOT NULL,
    embedding VECTOR(1536)
);

CREATE INDEX ON ai_insights USING hnsw (embedding vector_cosine_ops);
