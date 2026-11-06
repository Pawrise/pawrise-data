from datetime import datetime
from enum import Enum
from uuid import UUID

from pydantic import BaseModel, Field


class RoutingDecision(str, Enum):
    """Décision de routage après analyse des règles métier."""
    NORMAL = "NORMAL"
    CRITICAL_HANDOFF = "CRITICAL_HANDOFF"

class Activity(str, Enum):
    """Niveau d'activité remonté par le collier."""
    REST = "REST"
    WALKING = "WALKING"
    RUNNING = "RUNNING"
    AGITATED = "AGITATED"


class FsmState(str, Enum):
    """États de la machine à états du collier (contrat IoT)."""
    BOOT = "BOOT"
    PAIRING = "PAIRING"
    REST = "REST"
    ACTIVE = "ACTIVE"
    WALK = "WALK"
    ESCAPE = "ESCAPE"
    LIVE = "LIVE"
    LOW_POWER = "LOW POWER"
    NO_NETWORK = "NO NETWORK"


class BreedSize(str, Enum):
    """Catégorie de taille de race (enrichissement stream)."""
    SMALL = "small"
    MEDIUM = "medium"
    LARGE = "large"


class DogMetric(BaseModel):
    """Modèle de domaine pur pour une métrique."""
    anonymous_collar_id: UUID
    recorded_at: datetime
    bpm: int = Field(ge=20, le=400)
    respiratory_rate: int = Field(ge=1, le=150)
    temperature: float = Field(ge=30.0, le=45.0)
    activity: Activity
    fsm_state: FsmState
    weight_kg: float = Field(gt=0, le=120.0)
    age_months: int = Field(ge=0, le=360)
    breed_size: BreedSize
    bpm_moving_avg: float | None = Field(default=None, ge=20, le=400)
