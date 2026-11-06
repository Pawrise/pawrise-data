"""Tests du modèle `DogMetric` : valeurs acceptées et refusées."""

import pytest
from pydantic import ValidationError

from pawrise_data.domain.models import BreedSize, DogMetric


def test_valid_metric(make_metric):
    """Une mesure saine est acceptée."""
    metric = make_metric()
    assert isinstance(metric, DogMetric)
    assert metric.breed_size is BreedSize.MEDIUM


@pytest.mark.parametrize(
    "field,value",
    [
        ("bpm", 19),
        ("bpm", 401),
        ("respiratory_rate", 0),
        ("respiratory_rate", 151),
        ("temperature", 29.9),
        ("temperature", 45.1),
        ("weight_kg", 0),
        ("weight_kg", 120.1),
        ("age_months", -1),
        ("age_months", 361),
        ("breed_size", "giant"),
        ("activity", "FLYING"),
        ("fsm_state", "UNKNOWN"),
        ("anonymous_collar_id", "not-a-uuid"),
    ],
)
def test_invalid_values_rejected(make_metric, field, value):
    """Une valeur hors limites ou inconnue (bpm, température, poids, race...) est refusée."""
    with pytest.raises(ValidationError):
        make_metric(**{field: value})


def test_missing_enrichment_field_rejected(make_metric):
    """Une mesure sans donnée d'enrichissement (ex. le poids) est refusée."""
    data = make_metric().model_dump()
    del data["weight_kg"]
    with pytest.raises(ValidationError):
        DogMetric.model_validate(data)


def test_fsm_states_with_spaces(make_metric):
    """Les états du collier contenant un espace (LOW POWER, NO NETWORK) sont acceptés."""
    assert make_metric(fsm_state="LOW POWER").fsm_state.value == "LOW POWER"
    assert make_metric(fsm_state="NO NETWORK").fsm_state.value == "NO NETWORK"
