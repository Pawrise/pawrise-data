import os

import psycopg
from rich.console import Console
from rich.table import Table

from pawrise_data.domain.models import DogMetric, RoutingDecision
from pawrise_data.services.anomaly_detector import AnomalyDetector

# -- Configuration
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "pawrise_care")
DB_USER = os.getenv("DB_USER", "pawrise")
DB_PASSWORD = os.getenv("DB_PASSWORD", "pawrise123")
DSN = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# UUIDs de test correspondants au mock_generator.py
COLLAR_A = "11111111-1111-1111-1111-111111111111"
COLLAR_B = "22222222-2222-2222-2222-222222222222"

console = Console()

def get_latest_metric(cur, collar_id: str) -> DogMetric:
    """Récupère la toute dernière métrique enregistrée pour un collier."""
    # On utilise l'index sur (anonymous_collar_id, recorded_at DESC)
    cur.execute("""
        SELECT recorded_at, anonymous_collar_id, bpm, respiratory_rate, temperature 
        FROM dog_metrics 
        WHERE anonymous_collar_id = %s 
        ORDER BY recorded_at DESC 
        LIMIT 1
    """, (collar_id,))
    
    row = cur.fetchone()
    if not row:
        raise ValueError(f"Aucune métrique trouvée pour le collier {collar_id}")
        
    # Mapping vers notre modèle Pydantic
    return DogMetric(
        recorded_at=row[0],
        anonymous_collar_id=row[1],
        bpm=row[2],
        respiratory_rate=row[3],
        temperature=row[4]
    )

def main():
    console.print("\n[bold cyan]Pawrise Care - Test de l'Intercepteur de Red Flags[/bold cyan]\n")
    
    try:
        with psycopg.connect(DSN) as conn, conn.cursor() as cur:
            # Récupération des dernières données
            metric_a = get_latest_metric(cur, COLLAR_A)
            metric_b = get_latest_metric(cur, COLLAR_B)
            
            # Création d'une table Rich pour l'affichage
            table = Table(title="Résultats de l'analyse en temps réel (Handoff vs IA)")
            
            table.add_column("Collier ID", justify="left", style="cyan", no_wrap=True)
            table.add_column("Métrique (Dernier point)", justify="left", style="magenta")
            table.add_column("Décision du Service", justify="center", style="bold")
            table.add_column("Action Conséquente", justify="left")
            
            for metric, name in [(metric_a, "Collier A (Chien Sain)"), (metric_b, "Collier B (Cas d'urgence)")]:
                
                # 🚀 Appel de notre logique métier (Le "Garde-fou")
                decision = AnomalyDetector.evaluate(metric)
                
                # Formatage des stats pour l'affichage
                stats = f"BPM: {metric.bpm}, Resp: {metric.respiratory_rate}, Temp: {metric.temperature}°C"
                
                if decision == RoutingDecision.CRITICAL_HANDOFF:
                    decision_str = "[bold red blink]CRITICAL_HANDOFF[/bold red blink]"
                    action_str = "[red]Blocage IA -> Alerte SMS Vétérinaire[/red]"
                else:
                    decision_str = "[bold green]NORMAL[/bold green]"
                    action_str = "[green]Autorisé -> Routage vers LangGraph (RAG)[/green]"
                    
                table.add_row(name, stats, decision_str, action_str)

            console.print(table)
            console.print("\n[bold]Règle d'or respectée[/bold] : L'IA ne lira jamais les métriques du Collier B !\n")

    except Exception as e:  # noqa: BLE001
        console.print(f"[bold red]Erreur de connexion : {e}[/bold red]")

if __name__ == "__main__":
    main()
