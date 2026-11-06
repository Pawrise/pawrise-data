"""Tests du garde-fou : quelles situations déclenchent (ou non) une urgence."""

import pytest

from pawrise_data.domain.models import RoutingDecision
from pawrise_data.services.anomaly_detector import AnomalyDetector

NORMAL = RoutingDecision.NORMAL
CRITICAL = RoutingDecision.CRITICAL_HANDOFF


def test_normal_metric(make_metric):
    """Un chien sain est classé NORMAL."""
    assert AnomalyDetector.evaluate(make_metric()) == NORMAL


@pytest.mark.parametrize(
    "size,limit", [("small", 170), ("medium", 150), ("large", 130)]
)
def test_tachycardia_with_fever_is_critical(make_metric, size, limit):
    """Cœur trop rapide + forte fièvre = urgence, quelle que soit la taille de race."""
    metric = make_metric(breed_size=size, bpm=limit + 1, temperature=40.5)
    assert AnomalyDetector.evaluate(metric) == CRITICAL


@pytest.mark.parametrize("size,limit", [("small", 170), ("medium", 150), ("large", 130)])
def test_tachycardia_at_threshold_is_normal(make_metric, size, limit):
    """Un bpm pile au seuil n'est pas une urgence (le seuil est strict)."""
    metric = make_metric(breed_size=size, bpm=limit, temperature=40.5)
    assert AnomalyDetector.evaluate(metric) == NORMAL


def test_tachycardia_without_fever_is_normal(make_metric):
    """Un cœur rapide sans fièvre n'est pas une urgence à lui seul."""
    assert AnomalyDetector.evaluate(make_metric(bpm=200, temperature=39.0)) == NORMAL


def test_fever_exactly_at_threshold_is_normal(make_metric):
    """Une température pile au seuil n'est pas une urgence."""
    assert AnomalyDetector.evaluate(make_metric(bpm=200, temperature=40.0)) == NORMAL


@pytest.mark.parametrize("size,limit", [("small", 50), ("medium", 45), ("large", 40)])
def test_respiratory_distress_is_critical(make_metric, size, limit):
    """Une respiration trop rapide est une urgence ; pile au seuil, non."""
    assert AnomalyDetector.evaluate(make_metric(breed_size=size, respiratory_rate=limit + 1)) == CRITICAL
    assert AnomalyDetector.evaluate(make_metric(breed_size=size, respiratory_rate=limit)) == NORMAL


@pytest.mark.parametrize(
    "overrides",
    [
        {"activity": "RUNNING"},
        {"activity": "AGITATED"},
        {"fsm_state": "ESCAPE"},
    ],
)
def test_effort_context_raises_thresholds(make_metric, overrides):
    """En effort (course, agitation, fugue), les seuils sont relevés : un bpm élevé y est toléré."""
    # 160 bpm > 150 (medium au repos) mais < 150 * 1.35 = 202.5
    resting = make_metric(bpm=160, temperature=40.5)
    effort = make_metric(bpm=160, temperature=40.5, **overrides)
    assert AnomalyDetector.evaluate(resting) == CRITICAL
    assert AnomalyDetector.evaluate(effort) == NORMAL


def test_effort_resp_multiplier(make_metric):
    """En effort, la respiration tolérée double avant de déclencher l'urgence."""
    # medium: 45 au repos, 90 sous effort
    assert AnomalyDetector.evaluate(make_metric(respiratory_rate=80, activity="RUNNING")) == NORMAL
    assert AnomalyDetector.evaluate(make_metric(respiratory_rate=91, activity="RUNNING")) == CRITICAL


def test_effort_does_not_mask_extreme_values(make_metric):
    """Même en effort, des valeurs extrêmes restent une urgence."""
    metric = make_metric(bpm=250, temperature=41.0, activity="RUNNING")
    assert AnomalyDetector.evaluate(metric) == CRITICAL


def test_walking_is_not_effort(make_metric):
    """La marche ne compte pas comme un effort : les seuils restent ceux du repos."""
    assert AnomalyDetector.evaluate(make_metric(bpm=160, temperature=40.5, activity="WALKING")) == CRITICAL


def test_evaluate_is_stateless(make_metric):
    """Le garde-fou donne toujours la même réponse pour la même mesure (aucune mémoire)."""
    metric = make_metric(respiratory_rate=60)
    assert {AnomalyDetector.evaluate(metric) for _ in range(5)} == {CRITICAL}


def test_raw_spike_not_masked_by_smoothing(make_metric):
    """Un pic de bpm réel reste détecté même si la moyenne lissée est basse."""
    metric = make_metric(bpm=200, bpm_moving_avg=120.0, temperature=40.5)
    assert AnomalyDetector.evaluate(metric) == CRITICAL


def test_sustained_smoothed_tachycardia_is_critical(make_metric):
    """Une tachycardie durable (moyenne lissée haute) déclenche l'urgence même si le dernier bpm est plus bas."""
    metric = make_metric(bpm=140, bpm_moving_avg=160.0, temperature=40.5)
    assert AnomalyDetector.evaluate(metric) == CRITICAL


def test_moving_avg_below_threshold_is_normal(make_metric):
    """Bpm brut et moyenne sous le seuil : pas d'urgence, même avec de la fièvre."""
    metric = make_metric(bpm=100, bpm_moving_avg=110.0, temperature=40.5)
    assert AnomalyDetector.evaluate(metric) == NORMAL
