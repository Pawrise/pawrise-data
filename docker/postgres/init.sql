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
    activity TEXT NOT NULL CHECK (activity IN ('REST', 'WALKING', 'RUNNING', 'AGITATED')),
    fsm_state TEXT NOT NULL,
    weight_kg REAL NOT NULL CHECK (weight_kg > 0),
    age_months INTEGER NOT NULL CHECK (age_months >= 0),
    breed_size TEXT NOT NULL CHECK (breed_size IN ('small', 'medium', 'large')),
    UNIQUE (anonymous_collar_id, recorded_at)
);

-- 3. Transformation en Hypertable (TimescaleDB)
SELECT create_hypertable(
    'dog_metrics', 
    'recorded_at', 
    chunk_time_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

-- 3b. Compression columnar (chunks > 7 jours) et rétention (> 90 jours)
ALTER TABLE dog_metrics SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'anonymous_collar_id',
    timescaledb.compress_orderby = 'recorded_at DESC'
);

SELECT add_compression_policy('dog_metrics', INTERVAL '7 days', if_not_exists => TRUE);
SELECT add_retention_policy('dog_metrics', INTERVAL '90 days', if_not_exists => TRUE);

-- 3c. Agrégat continu journalier (optimisation des requêtes de graphiques Backend)
CREATE MATERIALIZED VIEW IF NOT EXISTS daily_dog_metrics
WITH (timescaledb.continuous) AS
SELECT
    anonymous_collar_id,
    time_bucket('1 day', recorded_at) AS bucket,
    AVG(bpm) AS avg_bpm,
    MIN(bpm) AS min_bpm,
    MAX(bpm) AS max_bpm,
    AVG(respiratory_rate) AS avg_respiratory_rate,
    MIN(respiratory_rate) AS min_respiratory_rate,
    MAX(respiratory_rate) AS max_respiratory_rate,
    AVG(temperature) AS avg_temperature,
    MIN(temperature) AS min_temperature,
    MAX(temperature) AS max_temperature
FROM dog_metrics
GROUP BY anonymous_collar_id, time_bucket('1 day', recorded_at)
WITH NO DATA;

-- Rafraîchissement automatique chaque nuit (fenêtre : 3 jours -> 1 heure)
SELECT add_continuous_aggregate_policy(
    'daily_dog_metrics',
    start_offset => INTERVAL '3 days',
    end_offset => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 day',
    if_not_exists => TRUE
);

-- 4. Index optimisé pour nos services métier (Handoff) et RAG
CREATE INDEX IF NOT EXISTS ix_dog_metrics_collar_time 
ON dog_metrics (anonymous_collar_id, recorded_at DESC);

-- 4b. Alertes critiques (Critical Handoff) : historique consultable en SQL par le module IA / le Backend.
-- Les mesures critiques ne sont jamais écrites dans dog_metrics ; un doublon Kafka est ignoré (UNIQUE).
CREATE TABLE IF NOT EXISTS critical_alerts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    recorded_at TIMESTAMPTZ NOT NULL,
    anonymous_collar_id UUID NOT NULL,
    bpm SMALLINT NOT NULL CHECK (bpm > 0),
    respiratory_rate SMALLINT NOT NULL CHECK (respiratory_rate > 0),
    temperature REAL NOT NULL,
    activity TEXT NOT NULL CHECK (activity IN ('REST', 'WALKING', 'RUNNING', 'AGITATED')),
    fsm_state TEXT NOT NULL,
    weight_kg REAL NOT NULL CHECK (weight_kg > 0),
    age_months INTEGER NOT NULL CHECK (age_months >= 0),
    breed_size TEXT NOT NULL CHECK (breed_size IN ('small', 'medium', 'large')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (anonymous_collar_id, recorded_at)
);

CREATE INDEX IF NOT EXISTS ix_critical_alerts_collar_time
ON critical_alerts (anonymous_collar_id, recorded_at DESC);

-- 5. Table pour l'architecture RAG (Résultats / Insights générés)
CREATE TABLE IF NOT EXISTS ai_insights (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    anonymous_collar_id UUID NOT NULL,
    generated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    context_summary TEXT NOT NULL,
    embedding VECTOR(1536)
);

CREATE INDEX ON ai_insights USING hnsw (embedding vector_cosine_ops);
