"""HTTP API：查询端点、错误归一化、错误不透出。"""

from __future__ import annotations

import logging

logger = logging.getLogger("tests.api")


def _post_event(client, **overrides):
    payload = {
        "event_id": "e1", "run_id": "run-1", "task_id": "task-1",
        "event_type": "START", "seq": 0, "occurred_at": 1000.0,
    }
    payload.update(overrides)
    logger.info("INPUT %s", payload)
    resp = client.post("/runs/events", json=payload)
    logger.info("DECISION %s %s", resp.status_code, resp.json())
    return resp


def test_register_and_list_runs(client):
    r = client.post("/tasks", json={
        "task_id": "task-1", "expected_period_seconds": 30,
        "lag_tolerance_seconds": 5, "recovery_grace_seconds": 2,
    })
    assert r.status_code == 201 and r.json()["task_id"] == "task-1"
    _post_event(client)
    _post_event(client, event_id="e2", run_id="run-2", seq=0)
    runs = client.get("/runs").json()
    assert {r["run_id"] for r in runs} == {"run-1", "run-2"}
    only = client.get("/runs", params={"task_id": "task-1"}).json()
    assert {r["run_id"] for r in only} == {"run-1", "run-2"}


def test_unknown_run_404(client):
    resp = client.get("/runs/missing")
    assert resp.status_code == 404
    body = resp.json()
    assert body["error_code"] == "UNKNOWN_RUN"
    assert set(body) == {"error_code", "message", "detail"}


def test_pydantic_validation_returns_normalized_shape(client):
    # 缺少必填字段时 FastAPI 的 RequestValidationError 也应归一为 INVALID_EVENT
    resp = client.post("/runs/events", json={"event_id": "x"})
    assert resp.status_code == 422
    assert resp.json()["error_code"] == "INVALID_EVENT"


def test_future_event_is_invalid(client, mutable_time):
    client.post("/tasks", json={"task_id": "task-1", "expected_period_seconds": 10})
    mutable_time.value = 1000.0
    resp = _post_event(client, occurred_at=10_000.0)
    assert resp.status_code == 422
    assert resp.json()["error_code"] == "INVALID_EVENT"
    assert "future" in resp.json()["message"]


def test_list_freshness_and_alerts_empty_views(client):
    client.post("/tasks", json={"task_id": "task-1", "expected_period_seconds": 10})
    assert client.get("/freshness").json()[0]["state"] == "NO_DATA"
    assert client.get("/alerts").json() == []
