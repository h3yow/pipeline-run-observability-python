"""状态收敛：乱序、重复、终态吸收与并发写入的一致性。"""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor

from app.clock import ManualClock
from app.models import EventType, RejectReason, RunEvent, RunState
from app.store import RunStore

from tests.conftest import log_decision


def make_event(run_id="r1", ts=0.0, event_type=EventType.STARTED, seq=0, progress=None):
    return RunEvent(
        run_id=run_id,
        task_id="t1",
        event_type=event_type,
        occurred_at=ts,
        seq=seq,
        progress=progress,
        client_id="c1",
    )


def test_out_of_order_events_converge_uniquely(service):
    """乱序到达：无论顺序如何，最终状态唯一确定，过期事件被拒绝且原因可区分。"""
    # 直接用底层 store 验证纯收敛语义
    store = RunStore(ManualClock())
    # 按 逆序 投递：SUCCEEDED(30) -> PROGRESS(20) -> STARTED(10)
    sequence = [
        make_event(ts=30, event_type=EventType.SUCCEEDED),
        make_event(ts=20, event_type=EventType.PROGRESS, progress=50),
        make_event(ts=10, event_type=EventType.STARTED),
    ]
    decisions = []
    for event in sequence:
        d = store.ingest(event)
        log_decision("乱序", event, d)
        decisions.append(d)

    assert decisions[0].accepted is True
    assert decisions[0].current_state == RunState.SUCCEEDED
    # 后两个事件：运行已终结 -> TERMINAL_STALE
    assert decisions[1].accepted is False
    assert decisions[1].reject_reason == RejectReason.TERMINAL_STALE
    assert decisions[2].reject_reason == RejectReason.TERMINAL_STALE

    snap = store.get_snapshot("r1")
    assert snap.state == RunState.SUCCEEDED
    assert snap.last_event_type == EventType.SUCCEEDED
    assert snap.event_count == 1  # 被拒绝的事件不计入

    # 正序重放一遍，终态必须完全一致（结论与到达顺序无关）
    store2 = RunStore(ManualClock())
    for event in reversed(sequence):
        store2.ingest(event)
    assert store2.get_snapshot("r1").state == RunState.SUCCEEDED
    assert store2.get_snapshot("r1").event_count == 3  # 正序时全部有效


def test_duplicate_event_is_idempotent(service):
    """重复上报：同一事件身份第二次投递被去重，原因是 DUPLICATE_EVENT。"""
    store = RunStore(ManualClock())
    event = make_event(ts=10, event_type=EventType.PROGRESS, seq=1, progress=30)
    first = store.ingest(event)
    second = store.ingest(event.model_copy(deep=True))
    log_decision("重复", event, first)
    log_decision("重复", event, second)

    assert first.accepted is True
    assert second.accepted is False
    assert second.reject_reason == RejectReason.DUPLICATE_EVENT
    assert store.get_snapshot("r1").event_count == 1


def test_terminal_state_absorbs_late_events(service):
    """已终结运行不得被过期事件回退。"""
    store = RunStore(ManualClock())
    cases = [
        (EventType.SUCCEEDED, RunState.SUCCEEDED),
        (EventType.FAILED, RunState.FAILED),
        (EventType.CANCELED, RunState.CANCELED),
    ]
    for terminal_type, terminal_state in cases:
        run_id = f"run-{terminal_type.value}"
        store.ingest(make_event(run_id, 10, EventType.STARTED))
        d_term = store.ingest(make_event(run_id, 20, terminal_type))
        assert d_term.current_state == terminal_state

        late = make_event(run_id, 99, EventType.PROGRESS, seq=5, progress=10)
        d_late = store.ingest(late)
        log_decision("终态过期", late, d_late)
        assert d_late.accepted is False
        assert d_late.reject_reason == RejectReason.TERMINAL_STALE
        assert d_late.current_state == terminal_state


def test_stale_nonterminal_event_rejected_with_distinct_reason(service):
    """非终态运行中的乱序事件原因必须是 OUT_OF_ORDER，与其他原因可区分。"""
    store = RunStore(ManualClock())
    newer = make_event(ts=20, event_type=EventType.PROGRESS, seq=2, progress=80)
    older = make_event(ts=10, event_type=EventType.PROGRESS, seq=1, progress=10)
    store.ingest(newer)
    d = store.ingest(older)
    log_decision("乱序拒绝", older, d)
    assert d.accepted is False
    assert d.reject_reason == RejectReason.OUT_OF_ORDER
    # 被拒绝事件不得覆盖进度
    assert store.get_snapshot("r1").progress == 80


def test_concurrent_same_event_accepted_exactly_once(service):
    """并发去重：同一身份事件多线程同时投递，恰好只被接收一次。"""
    store = RunStore(ManualClock())
    event = make_event(ts=10, event_type=EventType.PROGRESS, seq=1, progress=40)
    copies = [event.model_copy(deep=True) for _ in range(16)]

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(store.ingest, copies))

    accepted = [r for r in results if r.accepted]
    rejected = [r for r in results if not r.accepted]
    assert len(accepted) == 1  # 恰好一次，不能因并发产生重复接收
    assert all(r.reject_reason == RejectReason.DUPLICATE_EVENT for r in rejected)
    assert store.get_snapshot("r1").event_count == 1


def test_concurrent_shuffled_events_converge_to_global_winner(service):
    """并发乱序：不同事件多线程投递，最终获胜者恒为全局最大顺序键，状态唯一。"""
    store = RunStore(ManualClock())
    events = [
        make_event(run_id="r2", ts=float(i), event_type=EventType.PROGRESS, seq=i,
                   progress=float(i))
        for i in range(20)
    ]
    # 全局最大键事件 SUCCEEDED(100, seq=9) 混入并发投递
    terminal = make_event("r2", 100.0, EventType.SUCCEEDED, seq=9)
    payload = events + [terminal]

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(store.ingest, payload))

    snap = store.get_snapshot("r2")
    # 不变量 1：全局最大键事件无论何时到达都必定接收并成为获胜者
    assert snap.state == RunState.SUCCEEDED
    assert snap.last_event_type == EventType.SUCCEEDED
    assert snap.last_occurred_at == 100.0
    # 不变量 2：每个身份最多接收一次（无并发脏写）
    accepted_identities = [
        (r.run_id,) for r in results if r.accepted
    ]
    accepted_events = [e for e, r in zip(payload, results) if r.accepted]
    identities = [
        (e.event_type, e.occurred_at, e.seq) for e in accepted_events
    ]
    assert len(identities) == len(set(identities))
    assert len(accepted_identities) == len(identities)
    # 不变量 3：被拒绝原因只能是 乱序 / 终态过期（终态抢先时）
    for r in results:
        if not r.accepted:
            assert r.reject_reason in (
                RejectReason.OUT_OF_ORDER,
                RejectReason.TERMINAL_STALE,
            )
