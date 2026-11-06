import pytest

from pawrise_data.domain.models import RoutingDecision
from pawrise_data.services.anomaly_detector import AnomalyDetector

NORMAL = RoutingDecision.NORMAL
CRITICAL = RoutingDecision.CRITICAL_HANDOFF


def test_normal_metric(make_metric):
    assert AnomalyDetector.evaluate(make_metric()) == NORMAL


@pytest.mark.parametrize(
    "size,limit", [("small", 170), ("medium", 150), ("large", 130)]
)
def test_tachycardia_with_fever_is_critical(make_metric, size, limit):
    metric = make_metric(breed_size=size, bpm=limit + 1, temperature=40.5)
    assert AnomalyDetector.evaluate(metric) == CRITICAL


@pytest.mark.parametrize("size,limit", [("small", 170), ("medium", 150), ("large", 130)])
def test_tachycardia_at_threshold_is_normal(make_metric, size, limit):
    metric = make_metric(breed_size=size, bpm=limit, temperature=40.5)
    assert AnomalyDetector.evaluate(metric) == NORMAL


def test_tachycardia_without_fever_is_normal(make_metric):
    assert AnomalyDetector.evaluate(make_metric(bpm=200, temperature=39.0)) == NORMAL


def test_fever_exactly_at_threshold_is_normal(make_metric):
    assert AnomalyDetector.evaluate(make_metric(bpm=200, temperature=40.0)) == NORMAL


@pytest.mark.parametrize("size,limit", [("small", 50), ("medium", 45), ("large", 40)])
def test_respiratory_distress_is_critical(make_metric, size, limit):
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
    # 160 bpm > 150 (medium au repos) mais < 150 * 1.35 = 202.5
    resting = make_metric(bpm=160, temperature=40.5)
    effort = make_metric(bpm=160, temperature=40.5, **overrides)
    assert AnomalyDetector.evaluate(resting) == CRITICAL
    assert AnomalyDetector.evaluate(effort) == NORMAL


def test_effort_resp_multiplier(make_metric):
    # medium: 45 au repos, 90 sous effort
    assert AnomalyDetector.evaluate(make_metric(respiratory_rate=80, activity="RUNNING")) == NORMAL
    assert AnomalyDetector.evaluate(make_metric(respiratory_rate=91, activity="RUNNING")) == CRITICAL


def test_effort_does_not_mask_extreme_values(make_metric):
    metric = make_metric(bpm=250, temperature=41.0, activity="RUNNING")
    assert AnomalyDetector.evaluate(metric) == CRITICAL


def test_walking_is_not_effort(make_metric):
    assert AnomalyDetector.evaluate(make_metric(bpm=160, temperature=40.5, activity="WALKING")) == CRITICAL


def test_evaluate_is_stateless(make_metric):
    metric = make_metric(respiratory_rate=60)
    assert {AnomalyDetector.evaluate(metric) for _ in range(5)} == {CRITICAL}
