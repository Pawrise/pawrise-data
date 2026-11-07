"""Consommateur Kafka : lit les mesures, déclenche les alertes urgentes et archive les mesures normales en base."""

import json
import logging
import os
import signal
import time

from confluent_kafka import Consumer, KafkaError, Producer
from rich.console import Console

from pawrise_data.domain.models import DogMetric, RoutingDecision
from pawrise_data.infrastructure import metrics as prom
from pawrise_data.infrastructure.db_repository import DatabaseRepository
from pawrise_data.services.anomaly_detector import AnomalyDetector

logger = logging.getLogger(__name__)

console = Console()

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
        """Configure la connexion Kafka (commit manuel des offsets) et prépare le lot de mesures en attente."""
        self.topic = topic
        self.db_repo = db_repo
        self.dlq_topic = dlq_topic
        self.alerts_topic = alerts_topic
        self.batch_size = batch_size
        self.flush_interval = flush_interval

        conf = {
            'bootstrap.servers': broker,
            'group.id': 'pawrise_data_group',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False,
        }
        self.consumer = Consumer(conf)
        self.producer = Producer({'bootstrap.servers': broker})
        self.running = False

        self._batch: list[tuple[DogMetric, bytes]] = []
        self._batch_started_at = 0.0
        self._pending_commit = False
        self._dlq_count = 0

    def _send_to_dlq(self, raw: bytes | None, reason: str) -> None:
        """Envoie un message inexploitable dans la DLQ (file des erreurs), avec la raison, pour ne rien perdre."""
        self._dlq_count += 1
        prom.MESSAGES_DLQ.inc()
        self.producer.produce(
            self.dlq_topic,
            value=raw,
            headers={'error': reason[:500].encode('utf-8')},
        )
        self.producer.poll(0)

    def _publish_alert(self, metric: DogMetric) -> None:
        """Publie une urgence (CRITICAL_HANDOFF) sur le topic des alertes critiques."""
        prom.CRITICAL_ALERTS.inc()
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
            inserted = 0
            start = time.perf_counter()
            try:
                self.db_repo.save_metrics_batch(metrics)
                inserted = len(metrics)
                console.print(f"[green]✅ Lot de {len(metrics)} données normales stockées.[/green]")
            except Exception as e:  # noqa: BLE001
                console.print(f"[red]❌ Échec du bulk insert, lot redirigé vers la DLQ : {e}[/red]")
                for _, raw in self._batch:
                    self._send_to_dlq(raw, f"db_error: {e}")
            elapsed = time.perf_counter() - start
            if inserted:
                prom.MESSAGES_INSERTED.inc(inserted)
                prom.BATCH_FLUSH_SECONDS.observe(elapsed)
            logger.info(
                "Lot traité : %d message(s) inséré(s) en %.3f s (%.1f ms) | %d message(s) envoyé(s) en DLQ",
                inserted,
                elapsed,
                elapsed * 1000,
                self._dlq_count,
            )
            self._dlq_count = 0
            self._batch = []
        elif self._dlq_count:
            logger.info(
                "Lot traité : 0 message(s) inséré(s) | %d message(s) envoyé(s) en DLQ",
                self._dlq_count,
            )
            self._dlq_count = 0

        if self._pending_commit:
            self.producer.flush(5.0)
            self.consumer.commit(asynchronous=False)
            self._pending_commit = False
            self._update_lag()

    def _update_lag(self) -> None:
        """Met à jour la jauge de lag (messages restant à lire, toutes partitions assignées)."""
        try:
            lag = 0
            for tp in self.consumer.assignment():
                _, high = self.consumer.get_watermark_offsets(tp, timeout=1.0, cached=True)
                pos = next(
                    (p.offset for p in self.consumer.position([tp]) if p.offset >= 0),
                    None,
                )
                if pos is not None:
                    lag += max(high - pos, 0)
            prom.CONSUMER_LAG.set(lag)
        except Exception:  # noqa: BLE001
            logger.debug("Calcul du lag impossible", exc_info=True)

    def _handle_message(self, raw: bytes | None) -> None:
        """Traite un message : l'invalide va à la DLQ, l'urgent déclenche une alerte, le normal rejoint le lot à archiver."""
        if raw is None:
            self._send_to_dlq(raw, "invalid_message: empty payload")
            return
        try:
            metric = DogMetric.model_validate(json.loads(raw.decode('utf-8')))
        except Exception as e:  # noqa: BLE001
            console.print(f"[yellow]⚠️ Colis malformé redirigé vers la DLQ : {e}[/yellow]")
            self._send_to_dlq(raw, f"invalid_message: {e}")
            return

        decision = AnomalyDetector.evaluate(metric)

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
        """Boucle principale : lit les messages, archive par lots (taille ou délai atteint) et s'arrête proprement."""
        self.consumer.subscribe([self.topic])
        self.running = True
        console.print(f"[bold cyan]Démarrage du Kafka Consumer sur le topic '{self.topic}'...[/bold cyan]")
        console.print("[green]En attente de nouveaux colis... (Ctrl+C pour quitter)[/green]")

        try:
            while self.running:
                msg = self.consumer.poll(POLL_TIMEOUT)

                if msg is not None:
                    err = msg.error()
                    if err is not None:
                        if err.code() == KafkaError._PARTITION_EOF:
                            pass
                        elif err.code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                            console.print("[dim]Le topic n'existe pas encore, en attente du premier message...[/dim]")
                            time.sleep(2)
                        else:
                            console.print(f"[red]Erreur Kafka : {err}[/red]")
                            break
                    else:
                        self._handle_message(msg.value())
                        self._pending_commit = True

                batch_full = len(self._batch) >= self.batch_size
                batch_expired = (
                    bool(self._batch)
                    and time.monotonic() - self._batch_started_at >= self.flush_interval
                )
                idle_commit = self._pending_commit and not self._batch
                if batch_full or batch_expired or idle_commit:
                    self._flush_batch()

        except KeyboardInterrupt:
            console.print("\n[cyan]Arrêt demandé par l'utilisateur...[/cyan]")
        finally:
            self.running = False
            try:
                self._flush_batch()
            except Exception as e:  # noqa: BLE001
                console.print(f"[red]Flush final impossible : {e}[/red]")
            self.consumer.close()
            self.db_repo.close()
            console.print("[cyan]Consumer arrêté proprement.[/cyan]")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    prom.start_metrics_server()
    db_repository = DatabaseRepository(dsn=DSN)

    metrics_consumer = MetricsConsumer(
        broker=KAFKA_BROKER_URL,
        topic=KAFKA_TOPIC,
        db_repo=db_repository
    )

    def signal_handler(sig, frame):
        """Demande l'arrêt propre du consommateur (Ctrl+C / SIGTERM)."""
        metrics_consumer.running = False

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    metrics_consumer.start()
