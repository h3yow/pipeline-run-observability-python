"""并发写入：状态与告警结论必须一致、可复现。"""

from __future__ import annotations

import logging
import random
import threading

from app.alerts import AlertManager
from app.clock import Clock
from app.models import EventType, FaultType, AlertState
from app.run_store import RunStore
from app.models import RunEvent, RunStatus, AcceptReason

logger = logging.getLogger("tests.concurrency")


def _event(event_id, run_id, event_type, seq, ts, progress=None, error=None):
    return RunEvent(
        event_id=event_id, run_id=run_id, task_id="task-1",
        event_type=event_type, seq=seq, occurred_at=ts,
        progress=progress, error=error,
    )


def test_concurrent_ingest_converges_identically(client):
    """N 个线程以随机乱序并发上报同一 run 的事件，结论必须唯一。"""
    client.post("/tasks", json={
        "task_id": "task-1", "expected_period_seconds": 600,
        "lag_tolerance_seconds": 0, "recovery_grace_seconds": 0,
    })

    events = [
        {"event_id": "e0", "run_id": "run-1", "task_id": "task-1",
         "event_type": EventType.START.value, "seq": 0, "occurred_at": 1000.0},
        {"event_id": "e1", "run_id": "run-1", "task_id": "task-1",
         "event_type": EventType.PROGRESS.value, "seq": 1, "occurred_at": 1001.0,
         "progress": 50},
        {"event_id": "e2", "run_id": "run-1", "task_id": "task-1",
         "event_type": EventType.SUCCEEDED.value, "seq": 2, "occurred_at": 1002.0},
    ]
    # 每个事件被多个线程重复投递，制造并发重复
    payloads = events * 8
    random.Random(42).shuffle(payloads)

    codes = []
    barrier = threading.Barrier(len(payloads))

    def worker(payload):
        barrier.wait()
        resp = client.post("/runs/events", json=payload)
        codes.append((payload["event_id"], resp.status_code,
                      resp.json().get("error_code")))

    threads = [threading.Thread(target=worker, args=(p,)) for p in payloads]
    logger.info("INPUT concurrent payloads=%d events=%s", len(payloads),
                [(p["event_id"], p["seq"], p["event_type"]) for p in events])
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    logger.info("DECISIONS %s", sorted(codes))

    final = client.get("/runs/run-1").json()
    logger.info("FINAL STATE status=%s watermark=%s events=%s",
                final["status"], final["high_watermark"], final["event_count"])

    # 每个 event_id 恰好一次成功，其余全部 DUPLICATE
    accepted = {eid for eid, code, err in codes if code == 200}
    duplicates = [c for c in codes if c[1] == 409 and c[2] == "DUPLICATE_EVENT"]
    assert accepted == {"e0", "e1", "e2"}
    assert len(duplicates) == len(payloads) - 3
    # 状态唯一确定
    assert final["status"] == RunStatus.SUCCEEDED.value
    assert final["progress"] == 100
    assert final["event_count"] == 3


def test_concurrent_duplicate_events_never_double_accepted(service):
    """相同事件的高并发：最多一个 ACCEPTED，其余 DUPLICATE，无异常。"""
    store: RunStore = service._runs
    ev = _event("same", "run-x", EventType.START, 0, 1000.0)
    results: list[AcceptReason] = []
    lock = threading.Lock()
    barrier = threading.Barrier(30)

    def worker():
        barrier.wait()
        _, reason = store.ingest(ev)
        with lock:
            results.append(reason)

    threads = [threading.Thread(target=worker) for _ in range(30)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    logger.info("INPUT 30x identical event_id=same DECISION counts=%s",
                {r.value: results.count(r) for r in set(results)})
    assert results.count(AcceptReason.ACCEPTED) == 1
    assert results.count(AcceptReason.DUPLICATE) == 29


def test_concurrent_faults_fire_alert_once():
    manager = AlertManager(Clock(lambda: 100.0))
    fired: list[bool] = []
    barrier = threading.Barrier(100)

    def worker():
        barrier.wait()
        _, is_new = manager.report_fault("task-9", FaultType.STALLED, "lag")
        fired.append(is_new)

    threads = [threading.Thread(target=worker) for _ in range(100)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    logger.info("INPUT 100 concurrent faults fired_total=%s", sum(fired))
    assert sum(fired) == 1
    alerts = manager.list_alerts(AlertState.FIRING)
    assert len(alerts) == 1 and alerts[0].count == 100
