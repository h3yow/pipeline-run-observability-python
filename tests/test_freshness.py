"""新鲜度与滞后判定：边界稳定、抗抖动、时钟回拨安全。"""
from __future__ import annotations

from app.freshness import evaluate_freshness
from app.models import FreshnessStatus, TaskConfig


def make_cfg(**overrides):
    base = dict(
        task_id="t1",
        expected_period_seconds=10.0,
        grace_seconds=2.0,
        miss_after_periods=2.0,
        staleness_multiplier=2.0,
    )
    base.update(overrides)
    return TaskConfig(**base)


def test_first_report_ever_is_no_data():
    """从未上报：明确 NO_DATA，不产生滞后与误报。"""
    report = evaluate_freshness(make_cfg(), None, evaluated_at=1000.0)
    assert report.status == FreshnessStatus.NO_DATA
    assert report.lag_seconds is None
    assert report.last_seen_at is None


def test_within_grace_is_fresh():
    report = evaluate_freshness(make_cfg(), last_seen_at=1000.0, evaluated_at=1001.0)
    assert report.status == FreshnessStatus.FRESH
    assert report.lag_seconds == 0.0


def test_overdue_reports_positive_lag():
    # elapsed=15, grace=2 -> 滞后 13s，处于超期但未到错过阈值(20s)
    report = evaluate_freshness(make_cfg(), 1000.0, 1015.0)
    assert report.status == FreshnessStatus.OVERDUE
    assert report.lag_seconds == 13.0


def test_missed_after_threshold():
    # elapsed=20 恰好达到 period*miss_after_periods=20
    report = evaluate_freshness(make_cfg(), 1000.0, 1020.0)
    assert report.status == FreshnessStatus.MISSED
    assert report.lag_seconds == 18.0


def test_per_task_period_is_independent():
    """不同任务各自的期望周期：同一 elapsed 下，错过阈值随周期不同。"""
    cfg_a = make_cfg(expected_period_seconds=10.0, grace_seconds=0.0,
                     miss_after_periods=1.0)
    cfg_b = make_cfg(expected_period_seconds=100.0, grace_seconds=0.0,
                     miss_after_periods=1.0)
    assert evaluate_freshness(cfg_a, 0.0, 15.0).status == FreshnessStatus.MISSED
    # 同样 15s 无数据，对周期 100s 的任务只是超期，远未到“错过”
    assert evaluate_freshness(cfg_b, 0.0, 15.0).status == FreshnessStatus.OVERDUE


def test_clock_rollback_never_causes_negative_lag():
    """时钟回拨：evaluated_at < last_seen_at 时滞后钳为 0，结论 FRESH。"""
    report = evaluate_freshness(make_cfg(), last_seen_at=1000.0, evaluated_at=900.0)
    assert report.status == FreshnessStatus.FRESH
    assert report.lag_seconds == 0.0
    assert report.last_seen_at == 1000.0


def test_conclusion_is_stable_for_same_input():
    """同一数据反复评估，结论与滞后必须完全一致（不因抖动反复翻转）。"""
    cfg = make_cfg()
    first = evaluate_freshness(cfg, 1000.0, 1015.0)
    for _ in range(5):
        again = evaluate_freshness(cfg, 1000.0, 1015.0)
        assert again.status == first.status
        assert again.lag_seconds == first.lag_seconds
        assert again.reason == first.reason


def test_boundary_exactly_at_grace_is_fresh():
    """边界：elapsed 恰好等于 grace 仍为 FRESH（不抖动误报）。"""
    report = evaluate_freshness(make_cfg(), 1000.0, 1002.0)
    assert report.status == FreshnessStatus.FRESH
