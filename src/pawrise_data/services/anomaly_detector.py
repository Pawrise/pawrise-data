from pawrise_data.domain.models import (
    Activity,
    BreedSize,
    DogMetric,
    FsmState,
    RoutingDecision,
)


class AnomalyDetector:
    """
    Service déterministe (Règles Métier) qui intercepte la donnée 
    AVANT tout envoi vers le LLM / LangGraph.
    Garantit l'application stricte du principe 'IA en Cage'.
    """
    
    # Seuils critiques au repos, par taille de race (à affiner avec les vrais vétérinaires)
    CRITICAL_BPM_BY_SIZE: dict[BreedSize, int] = {
        BreedSize.SMALL: 170,
        BreedSize.MEDIUM: 150,
        BreedSize.LARGE: 130,
    }
    CRITICAL_RESP_BY_SIZE: dict[BreedSize, int] = {
        BreedSize.SMALL: 50,
        BreedSize.MEDIUM: 45,
        BreedSize.LARGE: 40,
    }
    CRITICAL_TEMP_THRESHOLD = 40.0

    # Contexte d'effort / stress : seuils significativement relevés
    EFFORT_ACTIVITIES: frozenset[Activity] = frozenset({Activity.RUNNING, Activity.AGITATED})
    EFFORT_FSM_STATES: frozenset[FsmState] = frozenset({FsmState.ESCAPE})
    EFFORT_BPM_MULTIPLIER = 1.35
    EFFORT_RESP_MULTIPLIER = 2.0

    @classmethod
    def _is_under_effort(cls, metric: DogMetric) -> bool:
        return metric.activity in cls.EFFORT_ACTIVITIES or metric.fsm_state in cls.EFFORT_FSM_STATES

    @classmethod
    def _thresholds(cls, metric: DogMetric) -> tuple[float, float]:
        """Retourne (seuil_bpm, seuil_resp) selon breed_size et contexte d'effort."""
        bpm_threshold: float = cls.CRITICAL_BPM_BY_SIZE[metric.breed_size]
        resp_threshold: float = cls.CRITICAL_RESP_BY_SIZE[metric.breed_size]
        if cls._is_under_effort(metric):
            bpm_threshold *= cls.EFFORT_BPM_MULTIPLIER
            resp_threshold *= cls.EFFORT_RESP_MULTIPLIER
        return bpm_threshold, resp_threshold

    @classmethod
    def evaluate(cls, metric: DogMetric) -> RoutingDecision:
        """
        Évalue les métriques et retourne un statut de routage.
        Si la situation est grave, on bloque l'IA et on alerte.
        Stateless : décision basée uniquement sur le payload.
        """
        bpm_threshold, resp_threshold = cls._thresholds(metric)

        # Règle 1 : Tachycardie sévère combinée à une forte fièvre
        if metric.bpm > bpm_threshold and metric.temperature > cls.CRITICAL_TEMP_THRESHOLD:
            return RoutingDecision.CRITICAL_HANDOFF

        # Règle 2 : Détresse respiratoire isolée
        if metric.respiratory_rate > resp_threshold:
            return RoutingDecision.CRITICAL_HANDOFF
            
        # ... (D'autres règles if/else déterministes peuvent être ajoutées ici)
        
        # Par défaut : La donnée est considérée sûre pour être contextualisée par l'IA
        return RoutingDecision.NORMAL
