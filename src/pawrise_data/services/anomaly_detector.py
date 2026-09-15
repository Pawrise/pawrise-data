from pawrise_data.domain.models import DogMetric, RoutingDecision

class AnomalyDetector:
    """
    Service déterministe (Règles Métier) qui intercepte la donnée 
    AVANT tout envoi vers le LLM / LangGraph.
    Garantit l'application stricte du principe 'IA en Cage'.
    """
    
    # Seuils critiques (à affiner avec les vrais vétérinaires)
    CRITICAL_BPM_THRESHOLD = 150
    CRITICAL_TEMP_THRESHOLD = 40.0
    CRITICAL_RESP_THRESHOLD = 45
    
    @classmethod
    def evaluate(cls, metric: DogMetric) -> RoutingDecision:
        """
        Évalue les métriques et retourne un statut de routage.
        Si la situation est grave, on bloque l'IA et on alerte.
        """
        
        # Règle 1 : Tachycardie sévère combinée à une forte fièvre
        if metric.bpm > cls.CRITICAL_BPM_THRESHOLD and metric.temperature > cls.CRITICAL_TEMP_THRESHOLD:
            return RoutingDecision.CRITICAL_HANDOFF
            
        # Règle 2 : Détresse respiratoire isolée
        if metric.respiratory_rate > cls.CRITICAL_RESP_THRESHOLD:
            return RoutingDecision.CRITICAL_HANDOFF
            
        # ... (D'autres règles if/else déterministes peuvent être ajoutées ici)
        
        # Par défaut : La donnée est considérée sûre pour être contextualisée par l'IA
        return RoutingDecision.NORMAL
