"""Tests du consommateur Kafka (Kafka simulé) : tri des messages, alertes, erreurs et commits."""

import json
from unittest.mock import MagicMock, patch

import pytest

from pawrise_data.infrastructure import kafka_consumer
from pawrise_data.infrastructure.kafka_consumer import MetricsConsumer


@pytest.fixture
def consumer():
    """Fournit un consommateur dont Kafka et la base sont simulés."""
    with patch.object(kafka_consumer, "Consumer"), patch.object(kafka_consumer, "Producer"):
        repo = MagicMock()
        c = MetricsConsumer(broker="x:9092", topic="t", db_repo=repo)
        yield c


def _raw(metric) -> bytes:
    """Convertit une mesure en message brut, comme reçu depuis Kafka."""
    return metric.model_dump_json().encode()


def test_manual_commit_configured():
    """Le commit automatique est désactivé : on valide les messages nous-mêmes."""
    with patch.object(kafka_consumer, "Consumer") as cons, patch.object(kafka_consumer, "Producer"):
        MetricsConsumer(broker="x:9092", topic="t", db_repo=MagicMock())
    assert cons.call_args.args[0]["enable.auto.commit"] is False


def test_normal_metric_is_batched(consumer, make_metric):
    """Une mesure normale rejoint le lot à archiver, sans alerte."""
    consumer._handle_message(_raw(make_metric()))
    assert len(consumer._batch) == 1
    consumer.producer.produce.assert_not_called()


def test_critical_metric_publishes_alert_and_is_not_stored(consumer, make_metric):
    """Une urgence est publiée en alerte et n'est pas archivée."""
    metric = make_metric(respiratory_rate=100)
    consumer._handle_message(_raw(metric))
    assert consumer._batch == []
    consumer.producer.produce.assert_called_once()
    args, kwargs = consumer.producer.produce.call_args
    assert args[0] == consumer.alerts_topic
    payload = json.loads(kwargs["value"])
    assert payload["decision"] == "CRITICAL_HANDOFF"
    assert payload["metric"]["anonymous_collar_id"] == str(metric.anonymous_collar_id)


def test_critical_metric_is_saved_in_alerts_table(consumer, make_metric):
    """Une urgence est aussi enregistrée dans la table des alertes, sans passer par le lot."""
    metric = make_metric(respiratory_rate=100)
    consumer._handle_message(_raw(metric))
    consumer.db_repo.save_alert.assert_called_once_with(metric)
    consumer.db_repo.save_metrics_batch.assert_not_called()


def test_alert_db_failure_still_publishes_alert_and_goes_to_dlq(consumer, make_metric):
    """Si la base est en panne, l'alerte Kafka part quand même et le message est gardé en DLQ."""
    consumer.db_repo.save_alert.side_effect = RuntimeError("db down")
    consumer._handle_message(_raw(make_metric(respiratory_rate=100)))
    topics = [c.args[0] for c in consumer.producer.produce.call_args_list]
    assert topics == [consumer.alerts_topic, consumer.dlq_topic]
    dlq_call = consumer.producer.produce.call_args_list[1]
    assert dlq_call.kwargs["headers"]["error"].startswith(b"alert_db_error")


def test_normal_metric_does_not_save_alert(consumer, make_metric):
    """Une mesure normale n'écrit rien dans la table des alertes."""
    consumer._handle_message(_raw(make_metric()))
    consumer.db_repo.save_alert.assert_not_called()


@pytest.mark.parametrize("raw", [b"garbage", b"{}", b"\xff", json.dumps({"bpm": 5}).encode()])
def test_malformed_message_goes_to_dlq(consumer, raw):
    """Un message illisible ou incomplet est envoyé dans la DLQ avec la raison."""
    consumer._handle_message(raw)
    assert consumer._batch == []
    args, kwargs = consumer.producer.produce.call_args
    assert args[0] == consumer.dlq_topic
    assert kwargs["value"] == raw
    assert kwargs["headers"]["error"].startswith(b"invalid_message")


def test_flush_saves_batch_then_commits(consumer, make_metric):
    """Le lot est enregistré en base, puis seulement ensuite validé côté Kafka."""
    consumer._handle_message(_raw(make_metric()))
    consumer._pending_commit = True
    consumer._flush_batch()
    consumer.db_repo.save_metrics_batch.assert_called_once()
    consumer.consumer.commit.assert_called_once_with(asynchronous=False)
    assert consumer._batch == []
    assert consumer._pending_commit is False


def test_db_failure_redirects_batch_to_dlq(consumer, make_metric):
    """Si la base est en panne, le lot part dans la DLQ : aucune donnée perdue."""
    consumer.db_repo.save_metrics_batch.side_effect = RuntimeError("db down")
    consumer._handle_message(_raw(make_metric()))
    consumer._handle_message(_raw(make_metric()))
    consumer._pending_commit = True
    consumer._flush_batch()
    dlq_calls = [c for c in consumer.producer.produce.call_args_list if c.args[0] == consumer.dlq_topic]
    assert len(dlq_calls) == 2
    assert dlq_calls[0].kwargs["headers"]["error"].startswith(b"db_error")
    consumer.consumer.commit.assert_called_once()


def test_flush_without_pending_commit_does_not_commit(consumer):
    """Sans message à valider, aucun commit n'est fait."""
    consumer._flush_batch()
    consumer.consumer.commit.assert_not_called()


def test_empty_payload_goes_to_dlq(consumer):
    """Un message vide est envoyé dans la DLQ."""
    consumer._handle_message(None)
    assert consumer._batch == []
    args, kwargs = consumer.producer.produce.call_args
    assert args[0] == consumer.dlq_topic
    assert kwargs["headers"]["error"].startswith(b"invalid_message")
