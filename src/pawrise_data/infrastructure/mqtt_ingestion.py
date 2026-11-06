"""Pont d'ingestion : reçoit les messages du collier (MQTT), les enrichit puis les envoie dans Kafka."""

import json
import os
import random
import signal
import threading
from collections import OrderedDict, deque
from datetime import UTC, datetime
from enum import Enum
from typing import Any, Literal
from uuid import UUID

import paho.mqtt.client as mqtt
from confluent_kafka import Producer
from pydantic import BaseModel, Field, ValidationError
from rich.console import Console

console = Console()

MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "pawrise/+/telemetry")
KAFKA_BROKER_URL = os.getenv("KAFKA_BROKER_URL", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "dog_metrics_topic")
BPM_WINDOW_SIZE = int(os.getenv("BPM_WINDOW_SIZE", "5"))
MAX_TRACKED_COLLARS = int(os.getenv("MAX_TRACKED_COLLARS", "10000"))


class Activity(str, Enum):
    """Niveau d'activité du chien tel qu'envoyé par le collier."""
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
    bpm_moving_avg: float | None = Field(default=None, gt=0)


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
        """Prépare le producteur Kafka, le client MQTT et la mémoire des moyennes mobiles (une fenêtre par collier)."""
        self._producer = Producer({"bootstrap.servers": KAFKA_BROKER_URL})
        self._client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
        self._client.on_connect = self._on_connect
        self._client.on_message = self._on_message
        self._stop = threading.Event()
        self._bpm_windows: OrderedDict[UUID, deque[int]] = OrderedDict()

    def _bpm_moving_avg(self, collar: CollarPayload) -> float | None:
        """Moyenne mobile du bpm par collier (état porté par l'ingestion, pas par le détecteur)."""
        if collar.bpm is None:
            return None
        window = self._bpm_windows.setdefault(collar.device_id, deque(maxlen=BPM_WINDOW_SIZE))
        self._bpm_windows.move_to_end(collar.device_id)
        while len(self._bpm_windows) > MAX_TRACKED_COLLARS:
            self._bpm_windows.popitem(last=False)
        window.append(collar.bpm)
        return round(sum(window) / len(window), 1)

    @staticmethod
    def _delivery_report(err: Any, msg: Any) -> None:
        """Appelé par Kafka après l'envoi : signale un échec de livraison."""
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
        """Appelé à la connexion MQTT : s'abonne aux messages des colliers si la connexion est acceptée."""
        if reason_code.is_failure:
            console.print(f"[red]Connexion MQTT refusée: {reason_code}[/red]")
            return
        client.subscribe(MQTT_TOPIC, qos=1)
        console.print(f"[green]MQTT connecté, abonné à[/green] {MQTT_TOPIC}")

    def _on_message(self, client: mqtt.Client, userdata: Any, msg: mqtt.MQTTMessage) -> None:
        """Traite un message collier : le valide, l'enrichit (profil + bpm lissé) puis le publie dans Kafka. Un message invalide est écarté."""
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
                    "bpm_moving_avg": self._bpm_moving_avg(collar),
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
        """Lance le pont jusqu'à Ctrl+C / SIGTERM, puis s'arrête proprement (envoi des messages en attente)."""
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
    """Point d'entrée : démarre le pont MQTT -> Kafka."""
    MqttToKafkaBridge().run()


if __name__ == "__main__":
    main()
