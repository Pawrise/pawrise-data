import json
import os
import signal
import time

from confluent_kafka import Consumer, KafkaError
from rich.console import Console

from pawrise_data.domain.models import DogMetric, RoutingDecision
from pawrise_data.infrastructure.db_repository import DatabaseRepository
from pawrise_data.services.anomaly_detector import AnomalyDetector

console = Console()

# Configuration
KAFKA_BROKER_URL = os.getenv("KAFKA_BROKER_URL", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "dog_metrics_topic")

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "pawrise_care")
DB_USER = os.getenv("DB_USER", "pawrise")
DB_PASSWORD = os.getenv("DB_PASSWORD", "pawrise123")
DSN = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


class MetricsConsumer:
    """
    Le 'Robot Trieur' (Kafka Consumer).
    Écoute le 'tapis roulant' (Kafka) en continu, vérifie les données
    et les range dans la base ou déclenche une alerte.
    """
    def __init__(self, broker: str, topic: str, db_repo: DatabaseRepository):
        self.topic = topic
        self.db_repo = db_repo
        
        # Configuration de confluent-kafka
        conf = {
            'bootstrap.servers': broker,
            'group.id': 'pawrise_data_group',
            'auto.offset.reset': 'earliest' # Commencer au début si nouveau
        }
        self.consumer = Consumer(conf)
        self.running = False

    def start(self):
        self.consumer.subscribe([self.topic])
        self.running = True
        console.print(f"[bold cyan]Démarrage du Kafka Consumer sur le topic '{self.topic}'...[/bold cyan]")
        console.print("[green]En attente de nouveaux colis... (Ctrl+C pour quitter)[/green]")

        try:
            while self.running:
                # On attend 1 seconde pour voir s'il y a un message
                msg = self.consumer.poll(1.0)
                
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # Fin de partition, on ignore
                        continue
                    elif msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                        # Le topic n'existe pas encore
                        console.print("[dim]Le topic n'existe pas encore, en attente du premier message...[/dim]")
                        time.sleep(2)
                        continue
                    else:
                        console.print(f"[red]Erreur Kafka : {msg.error()}[/red]")
                        break

                # 1. On récupère le colis brut (en texte JSON)
                val = msg.value().decode('utf-8')
                
                try:
                    data = json.loads(val)
                    # 2. Vérification de la forme avec le moule (Pydantic)
                    metric = DogMetric(**data)
                    
                    # 3. Passage au Videur (Règles métier)
                    decision = AnomalyDetector.evaluate(metric)
                    
                    # 4. Action en fonction de la décision
                    if decision == RoutingDecision.CRITICAL_HANDOFF:
                        console.print(f"[bold red blink]🚨 URGENCE DETECTEE (Collier {metric.anonymous_collar_id}) ![/bold red blink]")
                        console.print(f"[red]   -> BPM: {metric.bpm} | Resp: {metric.respiratory_rate} | Temp: {metric.temperature}°C[/red]")
                        console.print("[red]   -> ACTION : IA Bloquée, Alerte SMS Vétérinaire envoyée.[/red]\n")
                    else:
                        # Le colis est normal, on demande au Bibliothécaire de le ranger
                        self.db_repo.save_metric(metric)
                        console.print(f"[green]✅ Donnée normale stockée (Collier {metric.anonymous_collar_id}).[/green]")
                        
                except Exception as e:  # noqa: BLE001
                    console.print(f"[yellow]⚠️ Colis malformé ignoré : {e}[/yellow]")
                    
        except KeyboardInterrupt:
            console.print("\n[cyan]Arrêt demandé par l'utilisateur...[/cyan]")
        finally:
            # On ferme proprement
            self.running = False
            self.consumer.close()
            console.print("[cyan]Consumer arrêté proprement.[/cyan]")


if __name__ == "__main__":
    db_repository = DatabaseRepository(dsn=DSN)
    
    metrics_consumer = MetricsConsumer(
        broker=KAFKA_BROKER_URL,
        topic=KAFKA_TOPIC,
        db_repo=db_repository
    )
    
    # Gestion de l'arrêt propre (Ctrl+C)
    def signal_handler(sig, frame):
        metrics_consumer.running = False
    
    signal.signal(signal.SIGINT, signal_handler)
    
    metrics_consumer.start()
