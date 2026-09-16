from datetime import datetime
from enum import Enum
from uuid import UUID

from pydantic import BaseModel, Field


class RoutingDecision(str, Enum):
    """Décision de routage après analyse des règles métier."""
    NORMAL = "NORMAL"
    CRITICAL_HANDOFF = "CRITICAL_HANDOFF"

class DogMetric(BaseModel):
    """Modèle de domaine pur pour une métrique."""
    anonymous_collar_id: UUID
    recorded_at: datetime
    bpm: int = Field(gt=0)
    respiratory_rate: int = Field(gt=0)
    temperature: float
