"""Générateur de faux payloads matériels (collier) pour tests bout en bout.

Aucun accès BDD / Kafka : ce module ne fait que fabriquer des payloads MQTT
conformes à `CollarPayload` (cf. `infrastructure/mqtt_ingestion.py`).
"""

import random
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from typing import Any

# UUIDs fixes (enrichissement déterministe par device_id côté ingestion)
COLLAR_A = "11111111-1111-1111-1111-111111111111"  # Chien au repos
COLLAR_B = "22222222-2222-2222-2222-222222222222"  # Chien en fugue (ESCAPE)
COLLAR_C = "33333333-3333-3333-3333-333333333333"  # Urgence critique au repos
# Colliers dont le breed_size (dérivé du device_id par mock_enrich) est connu
COLLAR_MEDIUM = "00000006-0006-4006-8006-000000000006"  # breed_size = medium
COLLAR_LARGE = "00000009-0009-4009-8009-000000000009"  # breed_size = large


def _payload(
    device_id: str,
    *,
    temperature: float,
    activity: str,
    fsm_state: str,
    bpm: int,
    respiratory_rate: int,
) -> dict[str, Any]:
    """Assemble un message collier (format MQTT) à partir des valeurs données."""
    return {
        "device_id": device_id,
        "temperature": round(temperature, 1),
        "activity": activity,
        "fsm_state": fsm_state,
        "bpm": bpm,
        "respiratory_rate": respiratory_rate,
    }


def resting_dog(device_id: str = COLLAR_A) -> dict[str, Any]:
    """Chien au repos, constantes saines -> NORMAL attendu."""
    return _payload(
        device_id,
        temperature=random.uniform(38.0, 39.0),
        activity="REST",
        fsm_state="REST",
        bpm=random.randint(70, 100),
        respiratory_rate=random.randint(15, 30),
    )


def walking_dog(device_id: str = COLLAR_A) -> dict[str, Any]:
    """Chien en balade, constantes légèrement élevées -> NORMAL attendu."""
    return _payload(
        device_id,
        temperature=random.uniform(38.3, 39.2),
        activity="WALKING",
        fsm_state="WALK",
        bpm=random.randint(100, 125),
        respiratory_rate=random.randint(25, 38),
    )


def escaping_dog(device_id: str = COLLAR_B) -> dict[str, Any]:
    """Fugue (ESCAPE) : effort intense mais physiologiquement plausible.

    Au-dessus des seuils "repos" mais sous les seuils d'effort -> NORMAL attendu.
    """
    return _payload(
        device_id,
        temperature=random.uniform(39.3, 39.9),
        activity="RUNNING",
        fsm_state="ESCAPE",
        bpm=random.randint(150, 170),
        respiratory_rate=random.randint(50, 70),
    )


def escaping_dog_critical(device_id: str = COLLAR_B) -> dict[str, Any]:
    """Fugue avec détresse physiologique, même avec seuils d'effort -> CRITICAL_HANDOFF attendu."""
    return _payload(
        device_id,
        temperature=random.uniform(40.8, 41.5),
        activity="AGITATED",
        fsm_state="ESCAPE",
        bpm=random.randint(235, 260),
        respiratory_rate=random.randint(105, 130),
    )


def critical_at_rest(device_id: str = COLLAR_C) -> dict[str, Any]:
    """Chien au repos avec tachycardie + fièvre + détresse respiratoire -> CRITICAL_HANDOFF attendu."""
    return _payload(
        device_id,
        temperature=random.uniform(40.5, 41.5),
        activity="REST",
        fsm_state="REST",
        bpm=random.randint(185, 210),
        respiratory_rate=random.randint(60, 80),
    )


@dataclass(frozen=True)
class Scenario:
    """Un scénario de test : nom, résultat attendu, générateur, collier utilisé et nombre de messages."""
    name: str
    description: str
    expected: str  # NORMAL | CRITICAL_HANDOFF
    factory: Callable[[str], dict[str, Any]]
    device_id: str
    count: int


SCENARIOS: tuple[Scenario, ...] = (
    Scenario("rest", "Chien au repos", "NORMAL", resting_dog, COLLAR_A, 3),
    Scenario("walk", "Chien en balade", "NORMAL", walking_dog, COLLAR_A, 2),
    Scenario("escape", "Fugue (ESCAPE) sans détresse", "NORMAL", escaping_dog, COLLAR_B, 3),
    Scenario("escape_critical", "Fugue (ESCAPE) avec détresse", "CRITICAL_HANDOFF", escaping_dog_critical, COLLAR_B, 2),
    Scenario("critical_rest", "Urgence au repos", "CRITICAL_HANDOFF", critical_at_rest, COLLAR_C, 2),
    Scenario("escape_medium", "Fugue sans détresse (medium)", "NORMAL", escaping_dog, COLLAR_MEDIUM, 2),
    Scenario("escape_critical_medium", "Fugue critique (medium)", "CRITICAL_HANDOFF", escaping_dog_critical, COLLAR_MEDIUM, 2),
    Scenario("critical_rest_medium", "Urgence au repos (medium)", "CRITICAL_HANDOFF", critical_at_rest, COLLAR_MEDIUM, 2),
    Scenario("escape_large", "Fugue sans détresse (large)", "NORMAL", escaping_dog, COLLAR_LARGE, 2),
    Scenario("escape_critical_large", "Fugue critique (large)", "CRITICAL_HANDOFF", escaping_dog_critical, COLLAR_LARGE, 2),
    Scenario("critical_rest_large", "Urgence au repos (large)", "CRITICAL_HANDOFF", critical_at_rest, COLLAR_LARGE, 2),
)


def generate_scenarios(names: list[str] | None = None) -> Iterator[tuple[Scenario, dict[str, Any]]]:
    """Génère (scénario, payload) dans l'ordre, filtrable par nom de scénario."""
    for scenario in SCENARIOS:
        if names and scenario.name not in names:
            continue
        for _ in range(scenario.count):
            yield scenario, scenario.factory(scenario.device_id)
