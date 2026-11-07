"""Métriques Prometheus du pipeline Data (santé du consumer, pas les données capteurs)."""

import os

from prometheus_client import Counter, Gauge, Histogram, start_http_server

METRICS_PORT = int(os.getenv("METRICS_PORT", "8000"))

MESSAGES_INSERTED = Counter(
    "pawrise_messages_inserted_total",
    "Mesures normales insérées en base",
)
MESSAGES_DLQ = Counter(
    "pawrise_messages_dlq_total",
    "Messages redirigés vers la DLQ",
)
CRITICAL_ALERTS = Counter(
    "pawrise_critical_alerts_total",
    "Alertes CRITICAL_HANDOFF publiées",
)
BATCH_FLUSH_SECONDS = Histogram(
    "pawrise_batch_flush_seconds",
    "Durée d'insertion d'un lot en base",
    buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
)
CONSUMER_LAG = Gauge(
    "pawrise_consumer_lag",
    "Messages en attente dans Kafka pour le consumer (toutes partitions)",
)


def start_metrics_server(port: int = METRICS_PORT) -> None:
    """Expose /metrics en HTTP pour le scraping par Prometheus."""
    start_http_server(port)
