"""Test de fumée end-to-end : MQTT -> bridge -> Kafka -> consumer -> TimescaleDB.

Nécessite la vraie stack (`docker compose up -d`). Skippé si elle est injoignable.
"""
import json
import os
import socket
import subprocess
import sys
import time
from collections.abc import Iterator
from uuid import UUID, uuid4

import paho.mqtt.client as mqtt
import psycopg
import pytest

MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
KAFKA_HOST, _, KAFKA_PORT = os.getenv("KAFKA_BROKER_URL", "localhost:9092").partition(":")
DSN = "postgresql://{u}:{p}@{h}:{port}/{n}".format(
    u=os.getenv("DB_USER", "pawrise"),
    p=os.getenv("DB_PASSWORD", "pawrise123"),
    h=os.getenv("DB_HOST", "localhost"),
    port=os.getenv("DB_PORT", "5432"),
    n=os.getenv("DB_NAME", "pawrise_care"),
)
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))

TIMEOUT_S = 60.0

pytestmark = pytest.mark.e2e


def _reachable(host: str, port: int) -> bool:
    """Vrai si un service répond sur ce port réseau."""
    try:
        with socket.create_connection((host, port), timeout=1.0):
            return True
    except OSError:
        return False


@pytest.fixture(scope="module")
def stack() -> None:
    """Saute le test si la stack Docker (MQTT, Kafka, base) n'est pas démarrée."""
    down = [
        name
        for name, host, port in (
            ("mosquitto", MQTT_HOST, MQTT_PORT),
            ("kafka", KAFKA_HOST, int(KAFKA_PORT or 9092)),
            ("postgres", DB_HOST, DB_PORT),
        )
        if not _reachable(host, port)
    ]
    if down:
        pytest.skip(f"Stack Docker indisponible ({', '.join(down)}) : `docker compose up -d`")


@pytest.fixture
def pipeline(stack: None) -> Iterator[None]:
    """Démarre les vrais entrypoints : pont MQTT->Kafka et consumer Kafka->DB."""
    procs = [
        subprocess.Popen(
            [sys.executable, "-m", module],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        for module in (
            "pawrise_data.infrastructure.mqtt_ingestion",
            "pawrise_data.infrastructure.kafka_consumer",
        )
    ]
    try:
        time.sleep(5)
        for p in procs:
            assert p.poll() is None, "un processus du pipeline s'est arrêté au démarrage"
        yield
    finally:
        for p in procs:
            p.terminate()
        for p in procs:
            try:
                p.wait(timeout=15)
            except subprocess.TimeoutExpired:
                p.kill()


def _publish(collar_id: UUID, payload: dict) -> None:
    """Publie un message collier sur MQTT, comme le ferait un vrai collier."""
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.connect(MQTT_HOST, MQTT_PORT)
    client.loop_start()
    try:
        info = client.publish(f"pawrise/{collar_id}/telemetry", json.dumps(payload), qos=1)
        info.wait_for_publish(10)
    finally:
        client.loop_stop()
        client.disconnect()


def _fetch_rows(collar_id: UUID) -> list[tuple]:
    """Relit en base les lignes enregistrées pour un collier."""
    with psycopg.connect(DSN) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT bpm, respiratory_rate, temperature, activity, fsm_state, "
            "weight_kg, age_months, breed_size "
            "FROM dog_metrics WHERE anonymous_collar_id = %s",
            (collar_id,),
        )
        return cur.fetchall()


def test_mqtt_to_timescaledb_smoke(pipeline: None) -> None:
    """Test de fumée : un message MQTT traverse tout le pipeline et arrive en base, enrichi."""
    collar_id = uuid4()
    _publish(
        collar_id,
        {
            "device_id": str(collar_id),
            "temperature": 38.5,
            "activity": "REST",
            "fsm_state": "REST",
            "bpm": 90,
            "respiratory_rate": 20,
        },
    )

    rows: list[tuple] = []
    deadline = time.monotonic() + TIMEOUT_S
    while time.monotonic() < deadline:
        rows = _fetch_rows(collar_id)
        if rows:
            break
        time.sleep(1)

    try:
        assert rows, f"aucune ligne stockée en {TIMEOUT_S}s pour {collar_id}"
        bpm, resp, temp, activity, fsm, weight, age, breed = rows[0]
        assert (bpm, resp, activity, fsm) == (90, 20, "REST", "REST")
        assert temp == pytest.approx(38.5)
        assert weight > 0
        assert age >= 0
        assert breed in {"small", "medium", "large"}
    finally:
        with psycopg.connect(DSN, autocommit=True) as conn:
            conn.execute("DELETE FROM dog_metrics WHERE anonymous_collar_id = %s", (collar_id,))
