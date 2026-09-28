"""状态收敛：重复上报、乱序到达、终态保护与可区分的拒绝原因。"""

from __future__ import annotations

import logging

from app.models import EventType, RunStatus

logger = logging.getLogger("tests.convergence")


def event(event_id, run_id="run-1", task_id="task-1", event_type=EventType.START,
          seq=0, occurred_at=1000.0, progress=None, error=None):
    return {
        "event_id": event_id, "run_id": run_id, "task_id": task_id,
        "event_type": event_type.value, "seq": seq, "occurred_at": occurred_at,
        "progress": progress, "error": error,
    }


def register_task(client, task_id="task-1", period=10, tolerance=0, grace=0):
    resp = client.post("/tasks", json={
        "task_id": task_id,
        "expected_period_seconds": period,
        "lag_tolerance_seconds": tolerance,
        "recovery_grace_seconds": grace,
    })
    assert resp.status_code == 201, resp.text


def get_status(client, run_id):
    return client.get(f"/runs/{run_id}").json()


def post(client, payload, expected_code=200):
    logger.info("INPUT POST /runs/events payload=%s", payload)
    resp = client.post("/runs/events", json=payload)
    logger.info(
        "DECISION status_code=%s body=%s", resp.status_code,
        resp.json() if resp.headers.get("content-type", "").startswith("application/json") else resp.text,
    )
    assert resp.status_code == expected_code, resp.text
    return resp.json()


def test_duplicate_event_id_is_rejected_and_idempotent(client):
    register_task(client)
    payload = event("e1", seq=0, occurred_at=1000.0)
    first = post(client, payload)
    assert first["status"] == RunStatus.RUNNING.value
    # 重复上报：相同 event_id 必须被拒绝且原因可区分
    body = post(client, payload, expected_code=409)
    assert body["error_code"] == "DUPLICATE_EVENT"
    assert body["detail"]["event_id"] == "e1"
    # 状态未受影响
    assert get_status(client, "run-1")["status"] == RunStatus.RUNNING.value


def test_out_of_order_events_converge_to_single_status(client):
    register_task(client)
    # SUCCEEDED(seq2) 先到，PROGRESS(seq1)、START(seq0) 后到
    post(client, event("e3", event_type=EventType.SUCCEEDED, seq=2, occurred_at=1002.0))
    post(client, event("e2", event_type=EventType.PROGRESS, seq=1, occurred_at=1001.0, progress=50))
    post(client, event("e1", seq=0, occurred_at=1000.0))
    final = get_status(client, "run-1")
    assert final["status"] == RunStatus.SUCCEEDED.value
    assert final["progress"] == 100
    assert final["high_watermark"] == 2


def test_convergence_is_order_independent(client, service):
    """正序与逆序到达得到完全一致的结论。"""
    register_task(client)
    ordered = [
        event("a1", run_id="A", seq=0, occurred_at=1000.0),
        event("a2", run_id="A", event_type=EventType.PROGRESS, seq=1, occurred_at=1001.0, progress=10),
        event("a3", run_id="A", event_type=EventType.SUCCEEDED, seq=2, occurred_at=1002.0),
    ]
    for p in ordered:
        post(client, p)
    forward = get_status(client, "A")

    register_task(client, task_id="task-2")
    reversed_events = [
        {**p, "run_id": "B", "task_id": "task-2"} for p in reversed(ordered)
    ]
    for p in reversed_events:
        post(client, p)
    backward = get_status(client, "B")

    assert forward["status"] == backward["status"] == RunStatus.SUCCEEDED.value
    assert forward["progress"] == backward["progress"] == 100


def test_terminal_state_is_not_rolled_back_by_late_event(client):
    register_task(client)
    post(client, event("e1", seq=0, occurred_at=1000.0))
    post(client, event("e2", event_type=EventType.SUCCEEDED, seq=1, occurred_at=1001.0))
    # 过期的 PROGRESS（seq 更大、到达更晚）不得把 SUCCEEDED 回退
    body = post(
        client,
        event("e3", event_type=EventType.PROGRESS, seq=2, occurred_at=1002.0, progress=10),
        expected_code=409,
    )
    assert body["error_code"] == "RUN_ALREADY_TERMINAL"
    assert body["detail"]["terminal_status"] == "SUCCEEDED"
    assert body["detail"]["event_seq"] == 2
    assert get_status(client, "run-1")["status"] == RunStatus.SUCCEEDED.value


def test_first_terminal_in_sequence_wins(client):
    """多个终态事件：按 seq 最早的终态定因，与到达顺序无关。"""
    register_task(client)
    post(client, event("cancel", event_type=EventType.CANCELLED, seq=3, occurred_at=1003.0))
    post(client, event("fail", event_type=EventType.FAILED, seq=2, occurred_at=1002.0, error="boom"))
    post(client, event("start", seq=0, occurred_at=1000.0))
    assert get_status(client, "run-1")["status"] == RunStatus.FAILED.value


def test_same_seq_conflict_is_rejected_as_stale(client):
    register_task(client)
    post(client, event("e1", seq=1, occurred_at=1000.0))
    body = post(client, event("eX", seq=1, occurred_at=1000.0), expected_code=409)
    assert body["error_code"] == "EVENT_STALE"
    assert body["detail"]["seq"] == 1


def test_invalid_payloads_are_normalized(client):
    register_task(client)
    # PROGRESS 缺 progress
    body = post(
        client,
        event("bad", event_type=EventType.PROGRESS, seq=0),
        expected_code=422,
    )
    assert body["error_code"] == "INVALID_EVENT"
    # FAILED 缺 error
    body = post(
        client,
        event("bad2", event_type=EventType.FAILED, seq=0, occurred_at=1000.0),
        expected_code=422,
    )
    assert body["error_code"] == "INVALID_EVENT"
    # 未知任务
    resp = client.post("/runs/events", json=event("e", task_id="nope"))
    assert resp.status_code == 404 and resp.json()["error_code"] == "UNKNOWN_TASK"


def test_orphan_progress_is_retained_but_not_applied(client):
    register_task(client)
    result = post(client, event("p1", event_type=EventType.PROGRESS, seq=0, progress=5))
    assert result["accepted"] is True
    assert result["detail"]["ignored_by_convergence"] is True
    assert get_status(client, "run-1")["status"] == RunStatus.PENDING.value
    # START 到达后重放，孤立 PROGRESS 仍早于 START，但状态收敛到 RUNNING
    post(client, event("s1", event_type=EventType.START, seq=1, occurred_at=1001.0))
    assert get_status(client, "run-1")["status"] == RunStatus.RUNNING.value
