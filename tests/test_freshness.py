"""新鲜度与滞后：首次上报、无数据、防抖、迟滞恢复、时钟回拨。"""

from __future__ import annotations

import logging

from app.models import EventType, FreshnessState

logger = logging.getLogger("tests.freshness")


def register_task(client, period, tolerance=0.0, grace=0.0, task_id="task-1"):
    resp = client.post("/tasks", json={
        "task_id": task_id,
        "expected_period_seconds": period,
        "lag_tolerance_seconds": tolerance,
        "recovery_grace_seconds": grace,
    })
    assert resp.status_code == 201, resp.text


def send_start(client, mutable_time, run_id="run-1", task_id="task-1", eid="e1", occurred_at=None):
    ts = mutable_time.value if occurred_at is None else occurred_at
    payload = {
        "event_id": eid, "run_id": run_id, "task_id": task_id,
        "event_type": EventType.START.value, "seq": 0, "occurred_at": ts,
    }
    logger.info("INPUT event=%s", payload)
    resp = client.post("/runs/events", json=payload)
    assert resp.status_code == 200, resp.text


def eval_freshness(client, task_id="task-1"):
    body = client.get(f"/tasks/{task_id}/freshness").json()
    logger.info(
        "DECISION task=%s state=%s lag=%s clock_skew=%s reason=%s",
        task_id, body["state"], body["lag_seconds"], body["clock_skew"], body["reason"],
    )
    return body


def test_no_data_before_first_event(client):
    register_task(client, period=10)
    view = eval_freshness(client)
    assert view["state"] == FreshnessState.NO_DATA.value
    assert view["lag_seconds"] is None
    assert view["reason"].startswith("NEVER_OBSERVED")
    # 无数据不得产生告警
    assert client.get("/alerts").json() == []


def test_first_event_is_never_overdue(client, mutable_time):
    register_task(client, period=10)
    send_start(client, mutable_time)  # t=1000
    mutable_time.value = 1009.0
    view = eval_freshness(client)
    assert view["state"] == FreshnessState.FRESH.value
    assert view["lag_seconds"] == 9.0


def test_tolerance_prevents_flapping(client, mutable_time):
    register_task(client, period=10, tolerance=2)
    send_start(client, mutable_time)
    # lag=11：破限仅 1s，处于宽限窗口，不告警
    mutable_time.value = 1011.0
    view = eval_freshness(client)
    assert view["state"] == FreshnessState.FRESH.value
    assert view["reason"].startswith("GRACE_WINDOW")
    assert client.get("/alerts?state=FIRING").json() == []
    # 破限持续 >= 2s：判超期
    mutable_time.value = 1013.0
    view = eval_freshness(client)
    assert view["state"] == FreshnessState.OVERDUE.value
    assert view["reason"].startswith("OVERDUE_DETECTED")


def test_stale_fires_once_and_recovers_then_refires(client, mutable_time):
    register_task(client, period=10)
    send_start(client, mutable_time)
    mutable_time.value = 1011.0
    eval_freshness(client)
    firing = client.get("/alerts?state=FIRING").json()
    assert len(firing) == 1
    # 反复评估不产生新告警（持续期内只计数）
    eval_freshness(client)
    eval_freshness(client)
    firing = client.get("/alerts?state=FIRING").json()
    assert len(firing) == 1 and firing[0]["count"] == 3
    assert client.get("/alerts").json().__len__() == 1

    # 新事件到达 -> 恢复
    mutable_time.value = 1011.0
    send_start(client, mutable_time, run_id="run-2", eid="e2")
    assert client.get("/alerts?state=RESOLVED").json()[0]["state"] == "RESOLVED"
    assert client.get("/alerts?state=FIRING").json() == []

    # 再次超期 -> 同一 key 重新触发一条 FIRING
    mutable_time.value = 2000.0
    eval_freshness(client)
    firing = client.get("/alerts?state=FIRING").json()
    assert len(firing) == 1 and firing[0]["count"] == 1


def test_recovery_grace_hysteresis(client, mutable_time):
    # recovery_grace=5：OVERDUE 后 lag 必须 <=15 才恢复
    register_task(client, period=10, grace=5)
    send_start(client, mutable_time)
    mutable_time.value = 1020.0
    assert eval_freshness(client)["state"] == FreshnessState.OVERDUE.value
    # 仅时间流逝不会恢复
    assert eval_freshness(client)["state"] == FreshnessState.OVERDUE.value


def test_clock_rollback_is_flagged_but_does_not_change_lag(client, mutable_time):
    register_task(client, period=10)
    send_start(client, mutable_time, occurred_at=1000.0)
    # 生产者时钟回拨 60s
    send_start(client, mutable_time, run_id="run-2", eid="e2", occurred_at=940.0)
    view = eval_freshness(client)
    assert view["clock_skew"] is True
    # lag 仍基于服务端时间，当前新鲜
    assert view["state"] == FreshnessState.FRESH.value


def test_unknown_task_returns_normalized_error(client):
    resp = client.get("/tasks/nope/freshness")
    assert resp.status_code == 404
    assert resp.json()["error_code"] == "UNKNOWN_TASK"
