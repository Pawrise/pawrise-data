"""Métriques Prometheus du pont MQTT -> Kafka (santé de l'ingestion, pas les données capteurs)."""

import os

from prometheus_client import Counter, Gauge, start_http_server

BRIDGE_METRICS_PORT = int(os.getenv("BRIDGE_METRICS_PORT", "8001"))

MQTT_RECEIVED = Counter(
    "pawrise_mqtt_messages_received_total",
    "Messages reçus des colliers sur MQTT",
)
MQTT_REJECTED = Counter(
    "pawrise_mqtt_messages_rejected_total",
    "Messages MQTT rejetés (JSON invalide ou validation Pydantic)",
)
MQTT_PUBLISHED = Counter(
    "pawrise_mqtt_messages_published_total",
    "Messages enrichis publiés sur Kafka",
)
KAFKA_DELIVERY_ERRORS = Counter(
    "pawrise_mqtt_kafka_delivery_errors_total",
    "Échecs de livraison Kafka côté pont MQTT",
)
TRACKED_COLLARS = Gauge(
    "pawrise_mqtt_tracked_collars",
    "Colliers dont le pont garde la moyenne mobile en mémoire",
)


def start_bridge_metrics_server(port: int = BRIDGE_METRICS_PORT) -> None:
    """Expose /metrics en HTTP pour le scraping par Prometheus."""
    start_http_server(port)
