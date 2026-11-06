import json
import os
import random
import signal
import threading
from datetime import UTC, datetime
from enum import Enum
from typing import Any, Literal
from uuid import UUID

import paho.mqtt.client as mqtt
from confluent_kafka import Producer
from pydantic import BaseModel, Field, ValidationError
from rich.console import Console

console = Console()

# Configuration
MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "pawrise/+/telemetry")
KAFKA_BROKER_URL = os.getenv("KAFKA_BROKER_URL", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "dog_metrics_topic")


class Activity(str, Enum):
    REST = "REST"
    WALKING = "WALKING"
    RUNNING = "RUNNING"
    AGITATED = "AGITATED"


class CollarPayload(BaseModel):
    """Payload brut publié par le collier sur MQTT."""
    device_id: UUID
    temperature: float
    activity: Activity
    fsm_state: str
    bpm: int | None = Field(default=None, gt=0)
    respiratory_rate: int | None = Field(default=None, gt=0)


class EnrichedMetric(BaseModel):
    """Data Contract publié sur `dog_metrics_topic` (Stream Enrichment)."""
    anonymous_collar_id: UUID
    recorded_at: datetime
    temperature: float
    activity: Activity
    fsm_state: str
    bpm: int | None = Field(default=None, gt=0)
    respiratory_rate: int | None = Field(default=None, gt=0)
    weight_kg: float = Field(gt=0)
    age_months: int = Field(ge=0)
    breed_size: Literal["small", "medium", "large"]


def mock_enrich(collar: CollarPayload) -> dict[str, Any]:
    """Simule la récupération des métadonnées chien (source réelle : Backend Rust).

    Déterministe par collier : un même `device_id` renvoie toujours le même profil.
    """
    rng = random.Random(collar.device_id.int)
    breed_size = rng.choice(("small", "medium", "large"))
    weight_range = {"small": (3.0, 10.0), "medium": (10.0, 25.0), "large": (25.0, 50.0)}[breed_size]
    return {
        "weight_kg": round(rng.uniform(*weight_range), 1),
        "age_months": rng.randint(2, 180),
        "breed_size": breed_size,
    }


class MqttToKafkaBridge:
    """Pont MQTT (Mosquitto) -> Kafka `dog_metrics_topic`."""

    def __init__(self) -> None:
        self._producer = Producer({"bootstrap.servers": KAFKA_BROKER_URL})
        self._client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self._client.on_connect = self._on_connect
        self._client.on_message = self._on_message
        self._stop = threading.Event()

    @staticmethod
    def _delivery_report(err: Any, msg: Any) -> None:
        if err is not None:
            console.print(f"[red]Échec de livraison Kafka: {err}[/red]")

    def _on_connect(
        self,
        client: mqtt.Client,
        userdata: Any,
        flags: Any,
        reason_code: Any,
        properties: Any = None,
    ) -> None:
        if reason_code.is_failure:
            console.print(f"[red]Connexion MQTT refusée: {reason_code}[/red]")
            return
        client.subscribe(MQTT_TOPIC, qos=1)
        console.print(f"[green]MQTT connecté, abonné à[/green] {MQTT_TOPIC}")

    def _on_message(self, client: mqtt.Client, userdata: Any, msg: mqtt.MQTTMessage) -> None:
        try:
            collar = CollarPayload.model_validate(json.loads(msg.payload.decode("utf-8")))
            metric = EnrichedMetric.model_validate(
                {
                    "anonymous_collar_id": collar.device_id,
                    "recorded_at": datetime.now(UTC),
                    "temperature": collar.temperature,
                    "activity": collar.activity,
                    "fsm_state": collar.fsm_state,
                    "bpm": collar.bpm,
                    "respiratory_rate": collar.respiratory_rate,
                    **mock_enrich(collar),
                }
            )
        except (json.JSONDecodeError, UnicodeDecodeError, ValidationError) as exc:
            console.print(f"[yellow]Message rejeté sur {msg.topic}: {exc}[/yellow]")
            return

        self._producer.produce(
            KAFKA_TOPIC,
            key=str(metric.anonymous_collar_id).encode("utf-8"),
            value=metric.model_dump_json().encode("utf-8"),
            callback=self._delivery_report,
        )
        self._producer.poll(0)
        console.print(f"[dim]{msg.topic} -> {KAFKA_TOPIC} ({metric.anonymous_collar_id})[/dim]")

    def run(self) -> None:
        signal.signal(signal.SIGINT, lambda *_: self._stop.set())
        signal.signal(signal.SIGTERM, lambda *_: self._stop.set())
        console.print(
            f"[bold cyan]Pawrise Care[/bold cyan] - MQTT ({MQTT_HOST}:{MQTT_PORT}) -> Kafka ({KAFKA_BROKER_URL})"
        )
        self._client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
        self._client.loop_start()
        try:
            while not self._stop.is_set():
                self._producer.poll(0.2)
        finally:
            self._client.loop_stop()
            self._client.disconnect()
            self._producer.flush(10)
            console.print("[bold]Ingestion arrêtée proprement.[/bold]")


def main() -> None:
    MqttToKafkaBridge().run()


if __name__ == "__main__":
    main()
