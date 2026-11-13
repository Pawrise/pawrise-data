"""Tests du dépôt de données (base simulée) : requêtes d'écriture et de lecture."""

from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest

from pawrise_data.infrastructure.db_repository import (
    INSERT_ALERT_QUERY,
    INSERT_METRIC_QUERY,
    DatabaseRepository,
)


@pytest.fixture
def pool_mock():
    """Remplace le pool de connexions par un faux."""
    with patch("pawrise_data.infrastructure.db_repository.ConnectionPool") as cls:
        yield cls.return_value


@pytest.fixture
def cursor(pool_mock):
    """Fournit un faux curseur SQL pour inspecter les requêtes envoyées."""
    cur = MagicMock()
    conn = MagicMock()
    conn.cursor.return_value.__enter__.return_value = cur
    pool_mock.connection.return_value.__enter__.return_value = conn
    return cur


@pytest.fixture
def repo(pool_mock) -> DatabaseRepository:
    """Fournit un dépôt branché sur le faux pool."""
    return DatabaseRepository("postgresql://test", min_size=2, max_size=7)


def test_init_opens_pool(pool_mock):
    """À la création, le pool de connexions est ouvert avec les bons réglages."""
    with patch("pawrise_data.infrastructure.db_repository.ConnectionPool") as cls:
        DatabaseRepository("postgresql://x", min_size=2, max_size=7)
    cls.assert_called_once_with("postgresql://x", min_size=2, max_size=7, open=False)
    cls.return_value.open.assert_called_once_with(wait=True)


def test_close_closes_pool(repo, pool_mock):
    """La fermeture du dépôt ferme le pool."""
    repo.close()
    pool_mock.close.assert_called_once()


def test_to_row_order_and_enum_values(make_metric):
    """Une mesure devient une ligne SQL dans le bon ordre, avec des valeurs texte pour les énumérations."""
    m = make_metric(activity="RUNNING", breed_size="large")
    row = DatabaseRepository._to_row(m)
    assert row == (
        m.recorded_at,
        m.anonymous_collar_id,
        m.bpm,
        m.respiratory_rate,
        m.temperature,
        "RUNNING",
        m.fsm_state,
        m.weight_kg,
        m.age_months,
        "large",
    )


def test_save_metric_executes_insert(repo, cursor, make_metric):
    """Enregistrer une mesure exécute un INSERT."""
    m = make_metric()
    repo.save_metric(m)
    cursor.execute.assert_called_once_with(
        INSERT_METRIC_QUERY, DatabaseRepository._to_row(m)
    )


def test_save_metric_propagates_errors(repo, cursor, make_metric):
    """Une erreur de base remonte à l'appelant (pas d'échec silencieux)."""
    cursor.execute.side_effect = RuntimeError("db down")
    with pytest.raises(RuntimeError):
        repo.save_metric(make_metric())


def test_save_metrics_batch_uses_executemany(repo, cursor, make_metric):
    """Un lot est inséré en une seule opération groupée."""
    metrics = [make_metric(), make_metric(bpm=100)]
    repo.save_metrics_batch(metrics)
    cursor.executemany.assert_called_once_with(
        INSERT_METRIC_QUERY, [DatabaseRepository._to_row(m) for m in metrics]
    )


def test_save_metrics_batch_empty_is_noop(repo, pool_mock):
    """Un lot vide ne touche pas à la base."""
    repo.save_metrics_batch([])
    pool_mock.connection.assert_not_called()


def test_save_alert_executes_idempotent_insert(repo, cursor, make_metric):
    """Une alerte est écrite dans critical_alerts, avec un INSERT qui ignore les doublons."""
    m = make_metric(respiratory_rate=100)
    repo.save_alert(m)
    cursor.execute.assert_called_once_with(
        INSERT_ALERT_QUERY, DatabaseRepository._to_row(m)
    )
    assert "critical_alerts" in INSERT_ALERT_QUERY
    assert "ON CONFLICT" in INSERT_ALERT_QUERY


def test_save_alert_propagates_errors(repo, cursor, make_metric):
    """Une erreur de base remonte à l'appelant."""
    cursor.execute.side_effect = RuntimeError("db down")
    with pytest.raises(RuntimeError):
        repo.save_alert(make_metric())


def test_get_history_builds_metrics(repo, cursor, make_metric):
    """L'historique relu en base redevient des mesures identiques aux originales."""
    m = make_metric(activity="WALKING", breed_size="small")
    cursor.fetchall.return_value = [DatabaseRepository._to_row(m)]
    collar_id = m.anonymous_collar_id

    result = repo.get_history(collar_id, days=7)

    args = cursor.execute.call_args.args
    assert args[1] == (collar_id, "7 days")
    assert len(result) == 1
    assert result[0] == m


def test_get_history_empty(repo, cursor):
    """Sans données, l'historique est vide ; la période par défaut est de 14 jours."""
    cursor.fetchall.return_value = []
    assert repo.get_history(uuid4()) == []
    assert cursor.execute.call_args.args[1][1] == "14 days"
