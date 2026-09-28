"""告警去重与恢复：持续故障一次告警，恢复后重新触发，并发不重复。"""
from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor

from app.alerts import AlertManager
from app.clock import ManualClock
from app.freshness import evaluate_freshness
from app.models import (
    FreshnessStatus,
    IncidentStatus,
    IncidentType,
    TaskConfig,
)

case_logger = logging.getLogger("observability.tests")


def make_report(cfg, last_seen, now):
    report = evaluate_freshness(cfg, last_seen, now)
    case_logger.info(
        "[告警] task=%s last_seen=%s now=%s -> status=%s lag=%s 依据=%s",
        cfg.task_id, last_seen, now, report.status.value,
        report.lag_seconds, report.reason,
    )
    return report


CFG = TaskConfig(
    task_id="t1",
    expected_period_seconds=10,
    grace_seconds=2,
    miss_after_periods=2.0,
)


def test_single_alert_during_persistent_failure(clock):
    """同一故障持续期间反复评估，只产生一次告警。"""
    manager = AlertManager(clock)
    first = manager.evaluate(make_report(CFG, 1000.0, 1015.0))
    assert len(first) == 1
    assert first[0].status == IncidentStatus.FIRING
    assert first[0].incident_type == IncidentType.STALE_DATA

    # 持续故障：后续多次评估不产生新告警
    for now in (1016.0, 1017.0, 1018.0):
        assert manager.evaluate(make_report(CFG, 1000.0, now)) == []
    firing = [i for i in manager.list_incidents("t1")
              if i.status == IncidentStatus.FIRING]
    assert len(firing) == 1


def test_resolve_then_refire_creates_new_incident(clock):
    """恢复后再次故障，必须重新触发新的告警。"""
    manager = AlertManager(clock)
    fired1 = manager.evaluate(make_report(CFG, 1000.0, 1015.0))[0]
    resolved = manager.evaluate(make_report(CFG, 1000.0, 1001.0))
    assert len(resolved) == 1
    assert resolved[0].incident_id == fired1.incident_id
    assert resolved[0].status == IncidentStatus.RESOLVED
    assert resolved[0].resolved_at is not None

    fired2 = manager.evaluate(make_report(CFG, 1001.0, 1030.0))
    assert len(fired2) == 1
    assert fired2[0].status == IncidentStatus.FIRING
    assert fired2[0].incident_id != fired1.incident_id

    all_incidents = manager.list_incidents("t1")
    assert len(all_incidents) == 2
    case_logger.info("[告警] 恢复后重新触发，历史告警数=%d", len(all_incidents))


def test_missed_run_is_distinct_incident_type(clock):
    """MISSED 对应 MISSED_RUN，与 STALE_DATA 是两种可区分故障。"""
    manager = AlertManager(clock)
    report = make_report(
        TaskConfig(task_id="t2", expected_period_seconds=10,
                   grace_seconds=0, miss_after_periods=1.0),
        1000.0, 1020.0,
    )
    assert report.status == FreshnessStatus.MISSED
    fired = manager.evaluate(report)
    assert fired[0].incident_type == IncidentType.MISSED_RUN


def test_concurrent_evaluation_opens_exactly_one_incident(clock):
    """并发评估同一持续故障：只能打开一条活动告警。"""
    manager = AlertManager(clock)
    bad_reports = [make_report(CFG, 1000.0, 1015.0) for _ in range(32)]
    with ThreadPoolExecutor(max_workers=8) as pool:
        transitions = list(pool.map(manager.evaluate, bad_reports))
    opened = [t for batch in transitions for t in batch
              if t.status == IncidentStatus.FIRING]
    firing = [i for i in manager.list_incidents("t1")
              if i.status == IncidentStatus.FIRING]
    assert len(opened) == 1
    assert len(firing) == 1
