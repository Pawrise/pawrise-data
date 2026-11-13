"""Tests du consommateur Kafka (Kafka simulé) : tri des messages, alertes, erreurs et commits."""

import json
from unittest.mock import MagicMock, patch

import pytest

from pawrise_data.infrastructure import kafka_consumer
from pawrise_data.infrastructure.kafka_consumer import MetricsConsumer


@pytest.fixture
def consumer():
    """Fournit un consommateur dont Kafka et la base sont simulés."""
    with (
        patch.object(kafka_consumer, "Consumer"),
        patch.object(kafka_consumer, "Producer"),
    ):
        repo = MagicMock()
        c = MetricsConsumer(broker="x:9092", topic="t", db_repo=repo)
        yield c


def _raw(metric) -> bytes:
    """Convertit une mesure en message brut, comme reçu depuis Kafka."""
    return metric.model_dump_json().encode()


def test_manual_commit_configured():
    """Le commit automatique est désactivé : on valide les messages nous-mêmes."""
    with (
        patch.object(kafka_consumer, "Consumer") as cons,
        patch.object(kafka_consumer, "Producer"),
    ):
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


@pytest.mark.parametrize(
    "raw", [b"garbage", b"{}", b"\xff", json.dumps({"bpm": 5}).encode()]
)
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
    dlq_calls = [
        c
        for c in consumer.producer.produce.call_args_list
        if c.args[0] == consumer.dlq_topic
    ]
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


def _kafka_msg(value: bytes | None = None, error=None):
    """Fabrique un faux message Kafka (valeur ou erreur)."""
    msg = MagicMock()
    msg.error.return_value = error
    msg.value.return_value = value
    return msg


def _kafka_error(code):
    """Fabrique une fausse erreur Kafka portant ce code."""
    err = MagicMock()
    err.code.return_value = code
    return err


def _stop_after(consumer, polls: list):
    """Programme les résultats successifs de poll() ; le dernier arrête la boucle."""

    def _poll(_timeout):
        """Renvoie le résultat suivant et coupe la boucle quand la liste est épuisée."""
        if len(polls) == 1:
            consumer.running = False
        item = polls.pop(0)
        if isinstance(item, BaseException):
            raise item
        return item

    consumer.consumer.poll.side_effect = _poll


def test_flush_with_only_dlq_messages_resets_counter(consumer):
    """Sans lot mais avec des messages en DLQ, le compteur est remis à zéro."""
    consumer._dlq_count = 3
    consumer._flush_batch()
    assert consumer._dlq_count == 0


def test_update_lag_sums_partitions(consumer):
    """Le lag est la somme des messages restant à lire sur les partitions assignées."""
    consumer.consumer.assignment.return_value = ["p0", "p1"]
    consumer.consumer.get_watermark_offsets.return_value = (0, 10)
    consumer.consumer.position.return_value = [MagicMock(offset=4)]
    with patch.object(kafka_consumer.prom, "CONSUMER_LAG") as gauge:
        consumer._update_lag()
    gauge.set.assert_called_once_with(12)


def test_update_lag_ignores_unknown_position(consumer):
    """Une position inconnue (offset négatif) n'ajoute rien au lag."""
    consumer.consumer.assignment.return_value = ["p0"]
    consumer.consumer.get_watermark_offsets.return_value = (0, 10)
    consumer.consumer.position.return_value = [MagicMock(offset=-1001)]
    with patch.object(kafka_consumer.prom, "CONSUMER_LAG") as gauge:
        consumer._update_lag()
    gauge.set.assert_called_once_with(0)


def test_update_lag_failure_is_swallowed(consumer):
    """Un calcul de lag impossible ne doit jamais faire tomber le consumer."""
    consumer.consumer.assignment.side_effect = RuntimeError("broker down")
    consumer._update_lag()


def test_start_archives_message_and_closes_cleanly(consumer, make_metric):
    """La boucle lit un message normal, l'archive, valide l'offset puis ferme proprement."""
    _stop_after(consumer, [_kafka_msg(_raw(make_metric()))])
    consumer.start()
    consumer.consumer.subscribe.assert_called_once_with(["t"])
    consumer.db_repo.save_metrics_batch.assert_called_once()
    consumer.consumer.commit.assert_called()
    consumer.consumer.close.assert_called_once()
    consumer.db_repo.close.assert_called_once()


def test_start_flushes_when_batch_is_full(consumer, make_metric):
    """Un lot plein est archivé sans attendre l'intervalle."""
    consumer.batch_size = 2
    consumer.flush_interval = 3600
    _stop_after(
        consumer,
        [_kafka_msg(_raw(make_metric())), _kafka_msg(_raw(make_metric())), None],
    )
    consumer.start()
    assert consumer.db_repo.save_metrics_batch.call_args_list[0].args[0].__len__() == 2


def test_start_ignores_partition_eof(consumer):
    """Une fin de partition n'est pas une erreur : la boucle continue."""
    err = _kafka_error(kafka_consumer.KafkaError._PARTITION_EOF)
    _stop_after(consumer, [_kafka_msg(error=err), None])
    consumer.start()
    consumer.consumer.close.assert_called_once()


def test_start_waits_when_topic_is_missing(consumer):
    """Si le topic n'existe pas encore, on patiente puis on réessaie."""
    err = _kafka_error(kafka_consumer.KafkaError.UNKNOWN_TOPIC_OR_PART)
    _stop_after(consumer, [_kafka_msg(error=err), None])
    with patch.object(kafka_consumer.time, "sleep") as sleep:
        consumer.start()
    sleep.assert_called_once_with(2)


def test_start_stops_on_fatal_kafka_error(consumer):
    """Une erreur Kafka grave arrête la boucle, avec fermeture propre."""
    err = _kafka_error(kafka_consumer.KafkaError._ALL_BROKERS_DOWN)
    _stop_after(consumer, [_kafka_msg(error=err), None])
    consumer.start()
    assert consumer.running is False
    consumer.consumer.close.assert_called_once()


def test_start_handles_keyboard_interrupt(consumer):
    """Ctrl+C arrête la boucle et ferme proprement."""
    _stop_after(consumer, [KeyboardInterrupt()])
    consumer.start()
    consumer.consumer.close.assert_called_once()


def test_start_survives_final_flush_failure(consumer, make_metric):
    """Si le flush final échoue, le consumer et la base sont quand même fermés."""
    consumer._handle_message(_raw(make_metric()))
    consumer._pending_commit = True
    consumer.consumer.commit.side_effect = RuntimeError("commit failed")
    _stop_after(consumer, [None])
    consumer.start()
    consumer.consumer.close.assert_called_once()
    consumer.db_repo.close.assert_called_once()
