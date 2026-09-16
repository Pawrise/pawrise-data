from uuid import UUID

import psycopg

from pawrise_data.domain.models import DogMetric


class DatabaseRepository:
    """
    Le 'Magasinier' (Data Access Object / Repository Pattern).
    Gère toutes les interactions avec notre base PostgreSQL / TimescaleDB.
    """
    def __init__(self, dsn: str):
        self.dsn = dsn

    def save_metric(self, metric: DogMetric) -> None:
        """
        [Opération d'Écriture] - 'Ranger les données'
        Sauvegarde une nouvelle métrique de santé validée.
        """
        query = """
            INSERT INTO dog_metrics (recorded_at, anonymous_collar_id, bpm, respiratory_rate, temperature)
            VALUES (%s, %s, %s, %s, %s)
        """
        # On ouvre la connexion, on exécute, on valide (commit) et on ferme
        with psycopg.connect(self.dsn) as conn, conn.cursor() as cur:
            cur.execute(
                query,
                (
                    metric.recorded_at,
                    metric.anonymous_collar_id,
                    metric.bpm,
                    metric.respiratory_rate,
                    metric.temperature
                )
            )
            conn.commit()

    def get_history(self, collar_id: UUID, days: int = 14) -> list[DogMetric]:
        """
        [Opération de Lecture] - 'Sortir les archives'
        Récupère l'historique d'un collier sur les X derniers jours (utile pour l'IA/RAG).
        """
        query = """
            SELECT recorded_at, anonymous_collar_id, bpm, respiratory_rate, temperature
            FROM dog_metrics
            WHERE anonymous_collar_id = %s
              AND recorded_at >= NOW() - INTERVAL %s
            ORDER BY recorded_at ASC
        """
        metrics = []
        with psycopg.connect(self.dsn) as conn, conn.cursor() as cur:
            cur.execute(query, (collar_id, f"{days} days"))
            rows = cur.fetchall()

            # On transforme chaque ligne de la base en objet DogMetric (Pydantic)
            for row in rows:
                metrics.append(DogMetric(
                    recorded_at=row[0],
                    anonymous_collar_id=row[1],
                    bpm=row[2],
                    respiratory_rate=row[3],
                    temperature=row[4]
                ))
        return metrics
