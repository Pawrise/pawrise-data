"""Tests du pont MQTT -> Kafka (brokers simulés) : validation, enrichissement et moyenne mobile."""

import json
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest
from pydantic import ValidationError

from pawrise_data.infrastructure import mqtt_ingestion
from pawrise_data.infrastructure.mqtt_ingestion import (
    CollarPayload,
    EnrichedMetric,
    MqttToKafkaBridge,
    mock_enrich,
)


@pytest.fixture
def bridge():
    """Fournit un pont dont MQTT et Kafka sont simulés."""
    with (
        patch.object(mqtt_ingestion, "Producer") as producer,
        patch.object(mqtt_ingestion.mqtt, "Client"),
    ):
        b = MqttToKafkaBridge()
        b._producer = producer.return_value
        yield b


def _msg(payload: bytes, topic: str = "pawrise/x/telemetry"):
    """Fabrique un faux message MQTT."""
    return SimpleNamespace(payload=payload, topic=topic)


def _payload(**overrides):
    """Fabrique un message collier valide (modifiable champ par champ)."""
    data = {
        "device_id": str(uuid4()),
        "temperature": 38.6,
        "activity": "REST",
        "fsm_state": "REST",
        "bpm": 90,
        "respiratory_rate": 20,
    }
    data.update(overrides)
    return json.dumps(data).encode()


def test_mock_enrich_is_deterministic():
    """Un même collier reçoit toujours le même profil (poids, âge, race)."""
    collar = CollarPayload.model_validate(json.loads(_payload()))
    assert mock_enrich(collar) == mock_enrich(collar)


def test_mock_enrich_respects_contract():
    """Le profil généré respecte toujours le contrat de données (poids > 0, âge >= 0, race valide)."""
    for _ in range(50):
        collar = CollarPayload.model_validate(json.loads(_payload()))
        data = mock_enrich(collar)
        assert data["breed_size"] in {"small", "medium", "large"}
        assert data["weight_kg"] > 0
        assert data["age_months"] >= 0


def test_collar_payload_rejects_bad_activity():
    """Une activité inconnue envoyée par le collier est refusée."""
    with pytest.raises(ValidationError):
        CollarPayload.model_validate(json.loads(_payload(activity="FLYING")))


def test_enriched_metric_rejects_bad_breed_size():
    """Une taille de race inconnue est refusée."""
    with pytest.raises(ValidationError):
        EnrichedMetric.model_validate(
            {
                "anonymous_collar_id": str(uuid4()),
                "recorded_at": "2026-01-01T00:00:00Z",
                "temperature": 38.0,
                "activity": "REST",
                "fsm_state": "REST",
                "weight_kg": 10,
                "age_months": 5,
                "breed_size": "huge",
            }
        )


def test_valid_message_is_published_to_kafka(bridge):
    """Un message valide part dans Kafka, enrichi et anonymisé (plus de device_id)."""
    bridge._on_message(None, None, _msg(_payload()))
    bridge._producer.produce.assert_called_once()
    kwargs = bridge._producer.produce.call_args.kwargs
    body = json.loads(kwargs["value"])
    assert {
        "anonymous_collar_id",
        "weight_kg",
        "age_months",
        "breed_size",
    } <= body.keys()
    assert "device_id" not in body
    assert kwargs["key"].decode() == body["anonymous_collar_id"]


@pytest.mark.parametrize(
    "raw",
    [
        b"not json",
        b"\xff\xfe",
        b"{}",
        _payload(activity="FLYING"),
        _payload(temperature="hot"),
    ],
)
def test_invalid_message_is_rejected(bridge, raw):
    """Un message invalide est écarté et n'est pas envoyé dans Kafka."""
    bridge._on_message(None, None, _msg(raw))
    bridge._producer.produce.assert_not_called()


def test_on_connect_subscribes(bridge):
    """Connexion MQTT réussie : le pont s'abonne aux colliers."""
    client = MagicMock()
    bridge._on_connect(client, None, None, SimpleNamespace(is_failure=False))
    client.subscribe.assert_called_once_with(mqtt_ingestion.MQTT_TOPIC, qos=1)


def test_on_connect_failure_does_not_subscribe(bridge):
    """Connexion refusée : pas d'abonnement."""
    client = MagicMock()
    bridge._on_connect(client, None, None, SimpleNamespace(is_failure=True))
    client.subscribe.assert_not_called()


def _collar(device_id, bpm):
    """Fabrique un message collier avec un bpm donné."""
    return mqtt_ingestion.CollarPayload.model_validate(
        {
            "device_id": device_id,
            "temperature": 38.6,
            "activity": "REST",
            "fsm_state": "REST",
            "bpm": bpm,
        }
    )


def test_bpm_moving_avg_sliding_window(bridge):
    """La moyenne mobile ne garde que les derniers bpm (fenêtre glissante)."""
    dev = uuid4()
    window = mqtt_ingestion.BPM_WINDOW_SIZE
    values = list(range(100, 100 + window + 2))
    avgs = [bridge._bpm_moving_avg(_collar(dev, v)) for v in values]
    assert avgs[0] == 100.0
    assert avgs[-1] == round(sum(values[-window:]) / window, 1)


def test_bpm_moving_avg_isolated_per_collar(bridge):
    """Chaque collier a sa propre moyenne : les chiens ne se mélangent pas."""
    a, b = uuid4(), uuid4()
    bridge._bpm_moving_avg(_collar(a, 200))
    assert bridge._bpm_moving_avg(_collar(b, 80)) == 80.0


def test_bpm_moving_avg_none_when_bpm_missing(bridge):
    """Sans bpm, il n'y a pas de moyenne."""
    assert bridge._bpm_moving_avg(_collar(uuid4(), None)) is None


def test_bpm_windows_are_bounded(bridge, monkeypatch):
    """La mémoire des moyennes est plafonnée : les colliers les plus anciens sont oubliés."""
    monkeypatch.setattr(mqtt_ingestion, "MAX_TRACKED_COLLARS", 3)
    for _ in range(10):
        bridge._bpm_moving_avg(_collar(uuid4(), 90))
    assert len(bridge._bpm_windows) == 3


def test_published_payload_contains_moving_avg(bridge):
    """Le message envoyé dans Kafka contient la moyenne mobile."""
    bridge._on_message(None, None, _msg(_payload()))
    sent = json.loads(bridge._producer.produce.call_args.kwargs["value"])
    assert sent["bpm_moving_avg"] == 90.0


def _counter(metric) -> float:
    """Lit la valeur courante d'un compteur Prometheus."""
    return metric._value.get()


def test_bridge_metrics_count_received_published_and_rejected(bridge):
    """Les compteurs Prometheus du pont suivent les messages reçus, publiés et rejetés."""
    prom = mqtt_ingestion.prom
    received, published, rejected = (
        _counter(prom.MQTT_RECEIVED),
        _counter(prom.MQTT_PUBLISHED),
        _counter(prom.MQTT_REJECTED),
    )
    bridge._on_message(None, None, _msg(_payload()))
    bridge._on_message(None, None, _msg(b"pas du json"))
    assert _counter(prom.MQTT_RECEIVED) == received + 2
    assert _counter(prom.MQTT_PUBLISHED) == published + 1
    assert _counter(prom.MQTT_REJECTED) == rejected + 1


def test_bridge_metrics_count_delivery_errors(bridge):
    """Un échec de livraison Kafka incrémente le compteur d'erreurs."""
    before = _counter(mqtt_ingestion.prom.KAFKA_DELIVERY_ERRORS)
    MqttToKafkaBridge._delivery_report("boom", None)
    assert _counter(mqtt_ingestion.prom.KAFKA_DELIVERY_ERRORS) == before + 1


def test_bridge_sends_mqtt_credentials_when_configured():
    """Le pont s'authentifie auprès de Mosquitto quand MQTT_USERNAME est défini."""
    with (
        patch.object(mqtt_ingestion, "Producer"),
        patch.object(mqtt_ingestion.mqtt, "Client") as client,
        patch.object(mqtt_ingestion, "MQTT_USERNAME", "bridge"),
        patch.object(mqtt_ingestion, "MQTT_PASSWORD", "secret"),
    ):
        MqttToKafkaBridge()
    client.return_value.username_pw_set.assert_called_once_with("bridge", "secret")


def test_bridge_stays_anonymous_without_credentials():
    """Sans MQTT_USERNAME, le pont ne tente aucune authentification."""
    with (
        patch.object(mqtt_ingestion, "Producer"),
        patch.object(mqtt_ingestion.mqtt, "Client") as client,
        patch.object(mqtt_ingestion, "MQTT_USERNAME", None),
    ):
        MqttToKafkaBridge()
    client.return_value.username_pw_set.assert_not_called()


def test_run_starts_metrics_connects_and_shuts_down_cleanly(bridge):
    """run() expose les métriques, connecte MQTT, puis s'arrête proprement sur demande."""
    bridge._stop.set()
    with (
        patch.object(mqtt_ingestion.signal, "signal"),
        patch.object(mqtt_ingestion.prom, "start_bridge_metrics_server") as metrics,
    ):
        bridge.run()
    metrics.assert_called_once()
    bridge._client.connect.assert_called_once()
    bridge._client.loop_start.assert_called_once()
    bridge._client.loop_stop.assert_called_once()
    bridge._client.disconnect.assert_called_once()
    bridge._producer.flush.assert_called_once_with(10)


def test_run_signal_handlers_request_stop(bridge):
    """Les signaux SIGINT/SIGTERM demandent l'arrêt du pont."""
    bridge._stop.set()
    with (
        patch.object(mqtt_ingestion.signal, "signal") as sig,
        patch.object(mqtt_ingestion.prom, "start_bridge_metrics_server"),
    ):
        bridge.run()
    bridge._stop.clear()
    sig.call_args_list[0].args[1]()
    assert bridge._stop.is_set()


def test_main_runs_the_bridge():
    """main() construit le pont et le lance."""
    with patch.object(mqtt_ingestion, "MqttToKafkaBridge") as bridge_cls:
        mqtt_ingestion.main()
    bridge_cls.return_value.run.assert_called_once()
