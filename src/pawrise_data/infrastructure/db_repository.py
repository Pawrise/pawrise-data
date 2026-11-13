"""Accès à la base TimescaleDB : écriture et lecture des mesures de santé."""

from uuid import UUID

from psycopg_pool import ConnectionPool

from pawrise_data.domain.models import DogMetric

INSERT_METRIC_QUERY = """
    INSERT INTO dog_metrics (
        recorded_at, anonymous_collar_id, bpm, respiratory_rate, temperature,
        activity, fsm_state, weight_kg, age_months, breed_size
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

INSERT_ALERT_QUERY = """
    INSERT INTO critical_alerts (
        recorded_at, anonymous_collar_id, bpm, respiratory_rate, temperature,
        activity, fsm_state, weight_kg, age_months, breed_size
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (anonymous_collar_id, recorded_at) DO NOTHING
"""


class DatabaseRepository:
    """
    Le 'Magasinier' (Data Access Object / Repository Pattern).
    Gère toutes les interactions avec notre base PostgreSQL / TimescaleDB
    via un pool de connexions partagé.
    """

    def __init__(self, dsn: str, min_size: int = 1, max_size: int = 5):
        """Ouvre un pool de connexions réutilisables vers la base."""
        self.dsn = dsn
        self.pool = ConnectionPool(
            dsn, min_size=min_size, max_size=max_size, open=False
        )
        self.pool.open(wait=True)

    def close(self) -> None:
        """Ferme proprement toutes les connexions du pool."""
        self.pool.close()

    @staticmethod
    def _to_row(metric: DogMetric) -> tuple:
        """Transforme une mesure en ligne SQL, dans l'ordre des colonnes de l'INSERT."""
        return (
            metric.recorded_at,
            metric.anonymous_collar_id,
            metric.bpm,
            metric.respiratory_rate,
            metric.temperature,
            metric.activity.value,
            metric.fsm_state,
            metric.weight_kg,
            metric.age_months,
            metric.breed_size.value,
        )

    def save_metric(self, metric: DogMetric) -> None:
        """
        [Opération d'Écriture] - 'Ranger les données'
        Sauvegarde une nouvelle métrique de santé validée.
        """
        with self.pool.connection() as conn, conn.cursor() as cur:
            cur.execute(INSERT_METRIC_QUERY, self._to_row(metric))

    def save_metrics_batch(self, metrics: list[DogMetric]) -> None:
        """
        [Opération d'Écriture en lot] - Bulk insert atomique (une seule transaction).
        """
        if not metrics:
            return
        with self.pool.connection() as conn, conn.cursor() as cur:
            cur.executemany(INSERT_METRIC_QUERY, [self._to_row(m) for m in metrics])

    def save_alert(self, metric: DogMetric) -> None:
        """
        [Opération d'Écriture] - Enregistre une mesure critique dans `critical_alerts`.
        Idempotent : une alerte rejouée par Kafka (même collier, même instant) est ignorée.
        """
        with self.pool.connection() as conn, conn.cursor() as cur:
            cur.execute(INSERT_ALERT_QUERY, self._to_row(metric))

    def get_history(self, collar_id: UUID, days: int = 14) -> list[DogMetric]:
        """
        [Opération de Lecture] - 'Sortir les archives'
        Récupère l'historique d'un collier sur les X derniers jours (utile pour l'IA/RAG).
        """
        query = """
            SELECT recorded_at, anonymous_collar_id, bpm, respiratory_rate, temperature,
                   activity, fsm_state, weight_kg, age_months, breed_size
            FROM dog_metrics
            WHERE anonymous_collar_id = %s
              AND recorded_at >= NOW() - INTERVAL %s
            ORDER BY recorded_at ASC
        """
        with self.pool.connection() as conn, conn.cursor() as cur:
            cur.execute(query, (collar_id, f"{days} days"))
            rows = cur.fetchall()

        return [
            DogMetric(
                recorded_at=row[0],
                anonymous_collar_id=row[1],
                bpm=row[2],
                respiratory_rate=row[3],
                temperature=row[4],
                activity=row[5],
                fsm_state=row[6],
                weight_kg=row[7],
                age_months=row[8],
                breed_size=row[9],
            )
            for row in rows
        ]
