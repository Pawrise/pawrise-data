"""Publie de faux payloads collier sur Mosquitto (MQTT) pour tester la chaîne complète.

Chaîne attendue : MQTT -> mqtt_ingestion.py -> Kafka dog_metrics_topic
-> kafka_consumer / anomaly_detector -> critical_alerts_topic (si urgence).

Usage :
    uv run python -m pawrise_data.scripts.mock_producer [--scenario rest escape ...] [--interval 1.0]
"""

import argparse
import json
import os
import time

import paho.mqtt.client as mqtt
from rich.console import Console

from pawrise_data.scripts.mock_generator import SCENARIOS, generate_scenarios

MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_USERNAME = os.getenv("MQTT_USERNAME")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD")

console = Console()


def main() -> None:
    """Lit les options, se connecte à MQTT et publie les scénarios de test choisis, avec un délai entre chaque message."""
    parser = argparse.ArgumentParser(description="Mock collier -> MQTT")
    parser.add_argument(
        "--scenario",
        nargs="*",
        choices=[s.name for s in SCENARIOS],
        help="Scénarios à jouer (défaut : tous, dans l'ordre)",
    )
    parser.add_argument("--interval", type=float, default=1.0, help="Délai entre messages (s)")
    args = parser.parse_args()

    console.print(f"[bold cyan]Pawrise Care[/bold cyan] - Mock Producer vers MQTT ({MQTT_HOST}:{MQTT_PORT})")

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    if MQTT_USERNAME:
        client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    try:
        client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
    except OSError as exc:
        console.print(f"[bold red]Connexion MQTT impossible: {exc}[/bold red]")
        return
    client.loop_start()

    try:
        for scenario, payload in generate_scenarios(args.scenario):
            topic = f"pawrise/{payload['device_id']}/telemetry"
            info = client.publish(topic, json.dumps(payload), qos=1)
            info.wait_for_publish(timeout=5)
            style = "bold red" if scenario.expected == "CRITICAL_HANDOFF" else "dim"
            console.print(
                f"[{style}]{scenario.name:<24} {payload['fsm_state']:<7} {payload['activity']:<8} "
                f"bpm={payload['bpm']:<3} resp={payload['respiratory_rate']:<3} "
                f"temp={payload['temperature']} -> attendu: {scenario.expected}[/{style}]"
            )
            time.sleep(args.interval)
        console.print("[green]Tous les payloads ont été publiés sur Mosquitto.[/green]")
    except KeyboardInterrupt:
        pass
    finally:
        client.loop_stop()
        client.disconnect()


if __name__ == "__main__":
    main()
