import json
import os
import signal
import time

from confluent_kafka import Consumer, KafkaError, Producer
from rich.console import Console

from pawrise_data.domain.models import DogMetric, RoutingDecision
from pawrise_data.infrastructure.db_repository import DatabaseRepository
from pawrise_data.services.anomaly_detector import AnomalyDetector

console = Console()

# Configuration
KAFKA_BROKER_URL = os.getenv("KAFKA_BROKER_URL", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "dog_metrics_topic")
KAFKA_DLQ_TOPIC = os.getenv("KAFKA_DLQ_TOPIC", "dog_metrics_dlq")
KAFKA_ALERTS_TOPIC = os.getenv("KAFKA_ALERTS_TOPIC", "critical_alerts_topic")

BATCH_SIZE = 50
BATCH_FLUSH_INTERVAL = 1.0
POLL_TIMEOUT = 0.2

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "pawrise_care")
DB_USER = os.getenv("DB_USER", "pawrise")
DB_PASSWORD = os.getenv("DB_PASSWORD", "pawrise123")
DSN = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"


class MetricsConsumer:
    """
    Le 'Robot Trieur' (Kafka Consumer).
    Écoute le 'tapis roulant' (Kafka) en continu, vérifie les données,
    les range par lots dans la base ou déclenche une alerte.
    Les offsets sont commités manuellement après traitement du lot.
    """
    def __init__(
        self,
        broker: str,
        topic: str,
        db_repo: DatabaseRepository,
        dlq_topic: str = KAFKA_DLQ_TOPIC,
        alerts_topic: str = KAFKA_ALERTS_TOPIC,
        batch_size: int = BATCH_SIZE,
        flush_interval: float = BATCH_FLUSH_INTERVAL,
    ):
        self.topic = topic
        self.db_repo = db_repo
        self.dlq_topic = dlq_topic
        self.alerts_topic = alerts_topic
        self.batch_size = batch_size
        self.flush_interval = flush_interval

        # Configuration de confluent-kafka
        conf = {
            'bootstrap.servers': broker,
            'group.id': 'pawrise_data_group',
            'auto.offset.reset': 'earliest',  # Commencer au début si nouveau
            'enable.auto.commit': False,  # Commit manuel après flush du lot
        }
        self.consumer = Consumer(conf)
        self.producer = Producer({'bootstrap.servers': broker})
        self.running = False

        self._batch: list[tuple[DogMetric, bytes]] = []
        self._batch_started_at = 0.0
        self._pending_commit = False

    def _send_to_dlq(self, raw: bytes | None, reason: str) -> None:
        self.producer.produce(
            self.dlq_topic,
            value=raw,
            headers={'error': reason[:500].encode('utf-8')},
        )
        self.producer.poll(0)

    def _publish_alert(self, metric: DogMetric) -> None:
        payload = {
            'decision': RoutingDecision.CRITICAL_HANDOFF.value,
            'metric': metric.model_dump(mode='json'),
        }
        self.producer.produce(
            self.alerts_topic,
            key=str(metric.anonymous_collar_id).encode('utf-8'),
            value=json.dumps(payload).encode('utf-8'),
        )
        self.producer.poll(0)

    def _flush_batch(self) -> None:
        """Persiste le lot courant, puis commit les offsets."""
        if self._batch:
            metrics = [m for m, _ in self._batch]
            try:
                self.db_repo.save_metrics_batch(metrics)
                console.print(f"[green]✅ Lot de {len(metrics)} données normales stockées.[/green]")
            except Exception as e:  # noqa: BLE001
                console.print(f"[red]❌ Échec du bulk insert, lot redirigé vers la DLQ : {e}[/red]")
                for _, raw in self._batch:
                    self._send_to_dlq(raw, f"db_error: {e}")
            self._batch = []

        if self._pending_commit:
            self.producer.flush(5.0)
            self.consumer.commit(asynchronous=False)
            self._pending_commit = False

    def _handle_message(self, raw: bytes) -> None:
        try:
            # 1. Parsing du colis brut puis vérification de la forme (Pydantic)
            metric = DogMetric.model_validate(json.loads(raw.decode('utf-8')))
        except Exception as e:  # noqa: BLE001
            console.print(f"[yellow]⚠️ Colis malformé redirigé vers la DLQ : {e}[/yellow]")
            self._send_to_dlq(raw, f"invalid_message: {e}")
            return

        # 2. Passage au Videur (Règles métier)
        decision = AnomalyDetector.evaluate(metric)

        # 3. Action en fonction de la décision
        if decision == RoutingDecision.CRITICAL_HANDOFF:
            console.print(f"[bold red blink]🚨 URGENCE DETECTEE (Collier {metric.anonymous_collar_id}) ![/bold red blink]")
            console.print(f"[red]   -> BPM: {metric.bpm} | Resp: {metric.respiratory_rate} | Temp: {metric.temperature}°C[/red]")
            self._publish_alert(metric)
            console.print(f"[red]   -> ACTION : IA Bloquée, alerte publiée sur '{self.alerts_topic}'.[/red]\n")
        else:
            if not self._batch:
                self._batch_started_at = time.monotonic()
            self._batch.append((metric, raw))

    def start(self):
        self.consumer.subscribe([self.topic])
        self.running = True
        console.print(f"[bold cyan]Démarrage du Kafka Consumer sur le topic '{self.topic}'...[/bold cyan]")
        console.print("[green]En attente de nouveaux colis... (Ctrl+C pour quitter)[/green]")

        try:
            while self.running:
                msg = self.consumer.poll(POLL_TIMEOUT)

                if msg is not None:
                    if msg.error():
                        if msg.error().code() == KafkaError._PARTITION_EOF:
                            # Fin de partition, on ignore
                            pass
                        elif msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                            # Le topic n'existe pas encore
                            console.print("[dim]Le topic n'existe pas encore, en attente du premier message...[/dim]")
                            time.sleep(2)
                        else:
                            console.print(f"[red]Erreur Kafka : {msg.error()}[/red]")
                            break
                    else:
                        self._handle_message(msg.value())
                        self._pending_commit = True

                batch_full = len(self._batch) >= self.batch_size
                batch_expired = (
                    bool(self._batch)
                    and time.monotonic() - self._batch_started_at >= self.flush_interval
                )
                # Sans lot en attente, on commit dès que DLQ/alertes sont produites
                idle_commit = self._pending_commit and not self._batch
                if batch_full or batch_expired or idle_commit:
                    self._flush_batch()

        except KeyboardInterrupt:
            console.print("\n[cyan]Arrêt demandé par l'utilisateur...[/cyan]")
        finally:
            # On vide le lot restant et on ferme proprement
            self.running = False
            try:
                self._flush_batch()
            except Exception as e:  # noqa: BLE001
                console.print(f"[red]Flush final impossible : {e}[/red]")
            self.consumer.close()
            self.db_repo.close()
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
    signal.signal(signal.SIGTERM, signal_handler)

    metrics_consumer.start()
