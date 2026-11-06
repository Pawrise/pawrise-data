import pytest
from pydantic import ValidationError

from pawrise_data.domain.models import BreedSize, DogMetric


def test_valid_metric(make_metric):
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
    with pytest.raises(ValidationError):
        make_metric(**{field: value})


def test_missing_enrichment_field_rejected(make_metric):
    data = make_metric().model_dump()
    del data["weight_kg"]
    with pytest.raises(ValidationError):
        DogMetric.model_validate(data)


def test_fsm_states_with_spaces(make_metric):
    assert make_metric(fsm_state="LOW POWER").fsm_state.value == "LOW POWER"
    assert make_metric(fsm_state="NO NETWORK").fsm_state.value == "NO NETWORK"
