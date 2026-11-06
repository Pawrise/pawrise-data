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
    with patch.object(mqtt_ingestion, "Producer") as producer, patch.object(mqtt_ingestion.mqtt, "Client"):
        b = MqttToKafkaBridge()
        b._producer = producer.return_value
        yield b


def _msg(payload: bytes, topic: str = "pawrise/x/telemetry"):
    return SimpleNamespace(payload=payload, topic=topic)


def _payload(**overrides):
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
    collar = CollarPayload.model_validate(json.loads(_payload()))
    assert mock_enrich(collar) == mock_enrich(collar)


def test_mock_enrich_respects_contract():
    for _ in range(50):
        collar = CollarPayload.model_validate(json.loads(_payload()))
        data = mock_enrich(collar)
        assert data["breed_size"] in {"small", "medium", "large"}
        assert data["weight_kg"] > 0
        assert data["age_months"] >= 0


def test_collar_payload_rejects_bad_activity():
    with pytest.raises(ValidationError):
        CollarPayload.model_validate(json.loads(_payload(activity="FLYING")))


def test_enriched_metric_rejects_bad_breed_size():
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
    bridge._on_message(None, None, _msg(_payload()))
    bridge._producer.produce.assert_called_once()
    kwargs = bridge._producer.produce.call_args.kwargs
    body = json.loads(kwargs["value"])
    assert {"anonymous_collar_id", "weight_kg", "age_months", "breed_size"} <= body.keys()
    assert "device_id" not in body
    assert kwargs["key"].decode() == body["anonymous_collar_id"]


@pytest.mark.parametrize(
    "raw",
    [b"not json", b"\xff\xfe", b"{}", _payload(activity="FLYING"), _payload(temperature="hot")],
)
def test_invalid_message_is_rejected(bridge, raw):
    bridge._on_message(None, None, _msg(raw))
    bridge._producer.produce.assert_not_called()


def test_on_connect_subscribes(bridge):
    client = MagicMock()
    bridge._on_connect(client, None, None, SimpleNamespace(is_failure=False))
    client.subscribe.assert_called_once_with(mqtt_ingestion.MQTT_TOPIC, qos=1)


def test_on_connect_failure_does_not_subscribe(bridge):
    client = MagicMock()
    bridge._on_connect(client, None, None, SimpleNamespace(is_failure=True))
    client.subscribe.assert_not_called()
