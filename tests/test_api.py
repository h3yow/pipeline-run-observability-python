"""HTTP 端到端：事件上报、状态查询、错误归一化与并发写入。"""
from __future__ import annotations

import json
import logging
from concurrent.futures import ThreadPoolExecutor

from app.models import EventType

case_logger = logging.getLogger("observability.tests")


def task_payload(**overrides):
    payload = dict(
        task_id="t1",
        expected_period_seconds=10.0,
        grace_seconds=2.0,
        miss_after_periods=2.0,
    )
    payload.update(overrides)
    return payload


def event_payload(run_id="r1", ts=1000.0, event_type="STARTED", seq=0,
                  progress=None, task_id="t1"):
    return {
        "run_id": run_id,
        "task_id": task_id,
        "event_type": event_type,
        "occurred_at": ts,
        "seq": seq,
        "progress": progress,
        "client_id": "c1",
    }


def test_duplicate_report_returns_distinguishable_reason(client):
    """重复上报走 HTTP：200 + accepted=false + DUPLICATE_EVENT。"""
    client.post("/tasks", json=task_payload())
    payload = event_payload(ts=1000.0, event_type="PROGRESS", seq=1, progress=10)
    first = client.post("/runs/events", json=payload)
    second = client.post("/runs/events", json=payload)
    case_logger.info("[API重复] 输入=%s 首次=%s 二次=%s",
                     payload, first.json(), second.json())

    assert first.status_code == 200 and first.json()["accepted"] is True
    body = second.json()
    assert second.status_code == 200
    assert body["accepted"] is False
    assert body["reject_reason"] == "DUPLICATE_EVENT"
    assert body["current_state"] == "RUNNING"


def test_out_of_order_and_terminal_reasons_distinguishable(client):
    """乱序 vs 终态过期：两类拒绝原因必须可区分。"""
    client.post("/tasks", json=task_payload())
    started = event_payload(ts=1000.0, event_type="STARTED")
    succeeded = event_payload(ts=1010.0, event_type="SUCCEEDED")
    late_progress = event_payload(ts=1005.0, event_type="PROGRESS", seq=0, progress=5)

    client.post("/runs/events", json=started)
    client.post("/runs/events", json=succeeded)
    terminal_resp = client.post("/runs/events", json=late_progress)
    case_logger.info("[API终态] 过期事件=%s -> %s", late_progress, terminal_resp.json())
    # 被判定拒绝的事件统一返回 200 + accepted=false，原因字段可区分
    assert terminal_resp.status_code == 200
    assert terminal_resp.json()["accepted"] is False
    assert terminal_resp.json()["reject_reason"] == "TERMINAL_STALE"

    # 另一个运行：先收新事件，再投更旧事件 -> OUT_OF_ORDER
    client.post("/runs/events", json=event_payload("r2", 1010.0, "PROGRESS", seq=2))
    stale_resp = client.post(
        "/runs/events", json=event_payload("r2", 1009.0, "PROGRESS", seq=1)
    )
    assert stale_resp.status_code == 200
    assert stale_resp.json()["accepted"] is False
    assert stale_resp.json()["reject_reason"] == "OUT_OF_ORDER"

    # 终态查询结果稳定
    snap = client.get("/runs/r1").json()
    assert snap["state"] == "SUCCEEDED"


def test_invalid_payload_is_normalized(client):
    """上报数据异常：归一为 INVALID_PAYLOAD，不透出底层异常。"""
    bad = event_payload()
    bad["event_type"] = "NOT_A_TYPE"
    resp = client.post("/runs/events", json=bad)
    assert resp.status_code == 422
    body = resp.json()
    assert body["error"] == "INVALID_PAYLOAD"
    assert "message" in body

    bad_progress = event_payload(run_id="r3", ts=1000.0, event_type="PROGRESS",
                                 progress=999)
    resp2 = client.post("/runs/events", json=bad_progress)
    assert resp2.status_code == 422
    assert resp2.json()["error"] == "INVALID_PAYLOAD"


def test_clock_skew_is_distinguishable(client):
    """事件时间远超服务器时间：归一为 CLOCK_SKEW。"""
    client.post("/tasks", json=task_payload())
    # 服务器夹具时钟从 1000 起，容差 60s
    resp = client.post("/runs/events", json=event_payload(ts=999999.0))
    assert resp.status_code == 422
    assert resp.json()["error"] == "CLOCK_SKEW"


def test_run_not_found_normalized(client):
    resp = client.get("/runs/missing")
    assert resp.status_code == 404
    assert resp.json()["error"] == "RUN_NOT_FOUND"


def test_full_lifecycle_freshness_and_recovery(client, clock):
    """端到端：开始 -> 超期告警 -> 恢复关闭 -> 再故障重开。"""
    client.post("/tasks", json=task_payload())
    client.post("/runs/events", json=event_payload(ts=1000.0))

    clock.advance(15)  # elapsed 15s: OVERDUE（miss 阈值 20s）
    fresh1 = client.get("/tasks/t1/freshness").json()
    assert fresh1["status"] == "OVERDUE"
    incidents = client.get("/incidents", params={"task_id": "t1"}).json()
    assert len(incidents) == 1 and incidents[0]["status"] == "FIRING"

    # 持续超期重复检查不产生新告警
    clock.advance(1)
    client.get("/tasks/t1/freshness")
    assert len(client.get("/incidents", params={"task_id": "t1"}).json()) == 1

    # 新事件到达 -> 恢复
    client.post(
        "/runs/events",
        json=event_payload(ts=1016.0, event_type="PROGRESS", seq=1, progress=80),
    )
    fresh2 = client.get("/tasks/t1/freshness").json()
    assert fresh2["status"] == "FRESH"
    incident = client.get("/incidents", params={"task_id": "t1"}).json()[0]
    assert incident["status"] == "RESOLVED"
    assert incident["resolved_at"] is not None

    # 再次长期无数据 -> 重新触发一条新告警
    clock.advance(25)
    client.get("/tasks/t1/freshness")
    all_incidents = client.get("/incidents", params={"task_id": "t1"}).json()
    assert len(all_incidents) == 2
    assert all_incidents[1]["status"] == "FIRING"
    assert all_incidents[1]["incident_id"] != all_incidents[0]["incident_id"]
    case_logger.info("[API生命周期] 告警轨迹=%s",
                     json.dumps(all_incidents, ensure_ascii=False))


def test_concurrent_duplicate_posts_accepted_once(client):
    """并发 POST 完全相同事件：HTTP 层并发下只接收一次，其余可解释为重复。"""
    client.post("/tasks", json=task_payload())
    payload = event_payload(ts=1000.0, event_type="PROGRESS", seq=1, progress=42)

    def post_one(_):
        return client.post("/runs/events", json=payload)

    with ThreadPoolExecutor(max_workers=8) as pool:
        responses = list(pool.map(post_one, range(16)))

    statuses = [r.status_code for r in responses]
    bodies = [r.json() for r in responses]
    accepted = [b for b in bodies if b.get("accepted") is True]
    duplicates = [b for b in bodies
                  if b.get("reject_reason") == "DUPLICATE_EVENT"]
    case_logger.info(
        "[API并发] 总数=%d http状态=%s 接收=%d 重复=%d",
        len(responses), {s: statuses.count(s) for s in set(statuses)},
        len(accepted), len(duplicates),
    )
    assert len(accepted) == 1
    assert len(duplicates) == 15
    snap = client.get("/runs/r1").json()
    assert snap["event_count"] == 1
    assert snap["progress"] == 42
