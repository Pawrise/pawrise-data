"""Tests des serveurs de métriques Prometheus (serveur HTTP simulé)."""

from unittest.mock import patch

from pawrise_data.infrastructure import bridge_metrics, metrics


def test_consumer_metrics_server_listens_on_given_port():
    """Le consumer expose /metrics sur le port demandé."""
    with patch.object(metrics, "start_http_server") as server:
        metrics.start_metrics_server(9123)
    server.assert_called_once_with(9123)


def test_bridge_metrics_server_listens_on_given_port():
    """Le pont MQTT expose /metrics sur le port demandé."""
    with patch.object(bridge_metrics, "start_http_server") as server:
        bridge_metrics.start_bridge_metrics_server(9124)
    server.assert_called_once_with(9124)
