import os
import json
import time
from datetime import datetime, timezone
from confluent_kafka import Producer
from rich.console import Console

# UUIDs de test correspondants
COLLAR_A = "11111111-1111-1111-1111-111111111111"  # Chien sain
COLLAR_B = "22222222-2222-2222-2222-222222222222"  # Handoff test

console = Console()

KAFKA_BROKER_URL = os.getenv("KAFKA_BROKER_URL", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "dog_metrics_topic")

def delivery_report(err, msg):
    """Callback appelé une fois le message délivré ou en échec."""
    if err is not None:
        console.print(f"[red]Échec de livraison: {err}[/red]")
    else:
        # Affichage minimaliste
        pass

def main():
    console.print(f"[bold cyan]Pawrise Care[/bold cyan] - Démarrage du Mock Producer vers Kafka ({KAFKA_BROKER_URL})")
    
    conf = {'bootstrap.servers': KAFKA_BROKER_URL}
    producer = Producer(conf)
    
    try:
        # Envoi de 3 données normales
        for i in range(3):
            metric_data = {
                "recorded_at": datetime.now(timezone.utc).isoformat(),
                "anonymous_collar_id": COLLAR_A,
                "bpm": 85 + i,
                "respiratory_rate": 20 + i,
                "temperature": 38.5
            }
            producer.produce(
                KAFKA_TOPIC,
                value=json.dumps(metric_data).encode('utf-8'),
                callback=delivery_report
            )
            console.print(f"[dim]Envoyé: Métrique normale Collier A[/dim]")
            producer.poll(0) # Déclenche les callbacks
            time.sleep(1)
            
        # Envoi de 1 donnée critique (Urgence)
        critical_data = {
            "recorded_at": datetime.now(timezone.utc).isoformat(),
            "anonymous_collar_id": COLLAR_B,
            "bpm": 180,
            "respiratory_rate": 60,
            "temperature": 41.2
        }
        producer.produce(
            KAFKA_TOPIC,
            value=json.dumps(critical_data).encode('utf-8'),
            callback=delivery_report
        )
        console.print(f"[bold red]Envoyé: Métrique CRITIQUE Collier B[/bold red]")
        producer.poll(0)
        
        # On attend que tout soit parti
        producer.flush()
        console.print("[green]Tous les messages ont été envoyés au tapis roulant ![/green]")
        
    except KeyboardInterrupt:
        pass
    except Exception as e:
        console.print(f"[bold red]Erreur: {e}[/bold red]")

if __name__ == "__main__":
    main()
