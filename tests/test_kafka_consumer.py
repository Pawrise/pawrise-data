import json
from unittest.mock import MagicMock, patch

import pytest

from pawrise_data.infrastructure import kafka_consumer
from pawrise_data.infrastructure.kafka_consumer import MetricsConsumer


@pytest.fixture
def consumer():
    with patch.object(kafka_consumer, "Consumer"), patch.object(kafka_consumer, "Producer"):
        repo = MagicMock()
        c = MetricsConsumer(broker="x:9092", topic="t", db_repo=repo)
        yield c


def _raw(metric) -> bytes:
    return metric.model_dump_json().encode()


def test_manual_commit_configured():
    with patch.object(kafka_consumer, "Consumer") as cons, patch.object(kafka_consumer, "Producer"):
        MetricsConsumer(broker="x:9092", topic="t", db_repo=MagicMock())
    assert cons.call_args.args[0]["enable.auto.commit"] is False


def test_normal_metric_is_batched(consumer, make_metric):
    consumer._handle_message(_raw(make_metric()))
    assert len(consumer._batch) == 1
    consumer.producer.produce.assert_not_called()


def test_critical_metric_publishes_alert_and_is_not_stored(consumer, make_metric):
    metric = make_metric(respiratory_rate=100)
    consumer._handle_message(_raw(metric))
    assert consumer._batch == []
    consumer.producer.produce.assert_called_once()
    args, kwargs = consumer.producer.produce.call_args
    assert args[0] == consumer.alerts_topic
    payload = json.loads(kwargs["value"])
    assert payload["decision"] == "CRITICAL_HANDOFF"
    assert payload["metric"]["anonymous_collar_id"] == str(metric.anonymous_collar_id)


@pytest.mark.parametrize("raw", [b"garbage", b"{}", b"\xff", json.dumps({"bpm": 5}).encode()])
def test_malformed_message_goes_to_dlq(consumer, raw):
    consumer._handle_message(raw)
    assert consumer._batch == []
    args, kwargs = consumer.producer.produce.call_args
    assert args[0] == consumer.dlq_topic
    assert kwargs["value"] == raw
    assert kwargs["headers"]["error"].startswith(b"invalid_message")


def test_flush_saves_batch_then_commits(consumer, make_metric):
    consumer._handle_message(_raw(make_metric()))
    consumer._pending_commit = True
    consumer._flush_batch()
    consumer.db_repo.save_metrics_batch.assert_called_once()
    consumer.consumer.commit.assert_called_once_with(asynchronous=False)
    assert consumer._batch == []
    assert consumer._pending_commit is False


def test_db_failure_redirects_batch_to_dlq(consumer, make_metric):
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
    consumer._flush_batch()
    consumer.consumer.commit.assert_not_called()
