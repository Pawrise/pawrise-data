"""Outils partagés par les tests."""

from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

import pytest

from pawrise_data.domain.models import DogMetric


@pytest.fixture
def make_metric():
    """Fournit une fabrique de mesures valides (chien sain par défaut) ; on ne change que les champs voulus."""
    def _make(**overrides: Any) -> DogMetric:
        """Construit une mesure en appliquant les valeurs personnalisées par-dessus les valeurs saines."""
        data: dict[str, Any] = {
            "anonymous_collar_id": uuid4(),
            "recorded_at": datetime.now(UTC),
            "bpm": 90,
            "respiratory_rate": 20,
            "temperature": 38.5,
            "activity": "REST",
            "fsm_state": "REST",
            "weight_kg": 20.0,
            "age_months": 36,
            "breed_size": "medium",
        }
        data.update(overrides)
        return DogMetric.model_validate(data)

    return _make
