import os
import random
import psycopg
from datetime import datetime, timedelta, timezone
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TimeElapsedColumn
from rich.console import Console

# -- Configuration
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "pawrise_care")
DB_USER = os.getenv("DB_USER", "pawrise")
DB_PASSWORD = os.getenv("DB_PASSWORD", "pawrise123")

DSN = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

# -- Cas de Test
# UUIDs fixes pour faciliter le requêtage
COLLAR_A = "11111111-1111-1111-1111-111111111111"  # Chien sain
COLLAR_B = "22222222-2222-2222-2222-222222222222"  # Handoff (Anomalie au 14ème jour)

DAYS_OF_HISTORY = 14
INTERVAL_MINUTES = 15

console = Console()

def generate_healthy_metrics(timestamp: datetime, collar_id: str) -> tuple:
    """Génère des métriques physiologiques saines."""
    bpm = random.randint(70, 100)
    respiratory_rate = random.randint(15, 30)
    temperature = round(random.uniform(38.0, 39.0), 1)
    return (timestamp, collar_id, bpm, respiratory_rate, temperature)

def generate_critical_metrics(timestamp: datetime, collar_id: str) -> tuple:
    """Génère des métriques physiologiques d'urgence (Red Flag)."""
    bpm = random.randint(160, 190)
    respiratory_rate = random.randint(50, 70)
    temperature = round(random.uniform(40.5, 41.5), 1)
    return (timestamp, collar_id, bpm, respiratory_rate, temperature)

def seed_database():
    console.print("[bold cyan]Pawrise Care[/bold cyan] - Lancement du générateur de Mock Data...")
    
    end_date = datetime.now(timezone.utc)
    start_date = end_date - timedelta(days=DAYS_OF_HISTORY)
    
    # Calcul du nombre total de points par collier
    total_points = int((DAYS_OF_HISTORY * 24 * 60) / INTERVAL_MINUTES)
    
    # Préparation des données
    data_to_insert = []
    
    current_time = start_date
    for i in range(total_points):
        # 1. Collier A : Toujours sain
        data_to_insert.append(generate_healthy_metrics(current_time, COLLAR_A))
        
        # 2. Collier B : Sain les 13 premiers jours, Urgence le 14ème jour
        if current_time >= end_date - timedelta(days=1):
            data_to_insert.append(generate_critical_metrics(current_time, COLLAR_B))
        else:
            data_to_insert.append(generate_healthy_metrics(current_time, COLLAR_B))
            
        current_time += timedelta(minutes=INTERVAL_MINUTES)

    # Insertion dans la base
    try:
        with psycopg.connect(DSN) as conn:
            with conn.cursor() as cur:
                console.print(f"[green]Connecté à la base de données {DB_NAME}[/green]")
                
                # Suppression des anciennes données pour ces colliers si relance du script
                cur.execute("DELETE FROM dog_metrics WHERE anonymous_collar_id IN (%s, %s)", (COLLAR_A, COLLAR_B))
                conn.commit()
                
                # Insertion par lot avec barre de progression Rich
                insert_query = """
                    INSERT INTO dog_metrics (recorded_at, anonymous_collar_id, bpm, respiratory_rate, temperature)
                    VALUES (%s, %s, %s, %s, %s)
                """
                
                with Progress(
                    SpinnerColumn(),
                    TextColumn("[progress.description]{task.description}"),
                    BarColumn(),
                    TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                    TimeElapsedColumn(),
                    console=console
                ) as progress:
                    task = progress.add_task("[cyan]Insertion des séries temporelles...", total=len(data_to_insert))
                    
                    # On insère chunk par chunk avec executemany
                    chunk_size = 500
                    for j in range(0, len(data_to_insert), chunk_size):
                        chunk = data_to_insert[j:j+chunk_size]
                        cur.executemany(insert_query, chunk)
                        progress.update(task, advance=len(chunk))
                        
                conn.commit()
                console.print(f"[bold green]Succès ![/bold green] {len(data_to_insert)} lignes insérées dans 'dog_metrics'.")
                console.print(f"🐶 [bold]Collier A (Sain)[/bold]: {COLLAR_A}")
                console.print(f"🚨 [bold]Collier B (Handoff test)[/bold]: {COLLAR_B}")

    except Exception as e:
        console.print(f"[bold red]Erreur de connexion ou d'insertion : {e}[/bold red]")

if __name__ == "__main__":
    seed_database()
