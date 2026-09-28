"""单次运行（run）的状态聚合与存储：基于已保留事件重放收敛唯一状态。"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field

from .clock import Clock
from .models import (
    AcceptReason,
    EventType,
    RunEvent,
    RunStatus,
    RunStateView,
)

# 事件类型 -> 其被应用后对应的运行状态
_EVENT_TO_STATUS = {
    EventType.START: RunStatus.RUNNING,
    EventType.PROGRESS: RunStatus.RUNNING,
    EventType.SUCCEEDED: RunStatus.SUCCEEDED,
    EventType.FAILED: RunStatus.FAILED,
    EventType.CANCELLED: RunStatus.CANCELLED,
}

_TERMINAL_STATUS = frozenset(
    {RunStatus.SUCCEEDED, RunStatus.FAILED, RunStatus.CANCELLED}
)


@dataclass
class StoredEvent:
    event: RunEvent
    received_at: float


@dataclass
class _ReplayResult:
    status: RunStatus
    applied_seq: int | None
    progress: int | None
    last_occurred_at: float | None
    last_received_at: float | None
    terminal_at: float | None
    rejected_sequences: set[int]
    # 重放中实际生效的终态事件序号；该序号之后的任何事件都被终态保护拒绝
    terminal_seq: int | None = None


@dataclass
class RunAggregate:
    run_id: str
    task_id: str
    # 按 seq 索引：同一 run 内 seq 唯一；event_id 做幂等
    events_by_seq: dict[int, StoredEvent] = field(default_factory=dict)
    event_ids: set[str] = field(default_factory=set)
    high_watermark: int = -1

    def ingest(self, event: RunEvent, received_at: float) -> AcceptReason:
        """
        收敛规则（与到达顺序无关）：
        1. event_id 已存在 -> DUPLICATE，幂等拒绝（重复上报）；
        2. seq 槽位已被其他 event_id 占用 -> STALE（乱序冲突，槽位以先到者为准）；
        3. 其余事件无论 seq 大小一律不可变地保留，按 seq 升序重放得到唯一状态。

        终态保护由重放保证：seq 最大的终态事件决定结局，终态之后的非终态
        （例如迟到的 PROGRESS）无法回退状态；服务层将其翻译为
        RUN_ALREADY_TERMINAL 以便调用方区分拒绝原因。
        """
        if event.event_id in self.event_ids:
            return AcceptReason.DUPLICATE

        existing = self.events_by_seq.get(event.seq)
        if existing is not None and existing.event.event_id != event.event_id:
            return AcceptReason.STALE

        self.events_by_seq[event.seq] = StoredEvent(event=event, received_at=received_at)
        self.event_ids.add(event.event_id)
        if event.seq > self.high_watermark:
            self.high_watermark = event.seq
        return AcceptReason.ACCEPTED

    def replay(self) -> _ReplayResult:
        """按 seq 升序重放全部保留事件，推导唯一的最终状态。"""
        status = RunStatus.PENDING
        applied_seq: int | None = None
        progress: int | None = None
        last_occurred: float | None = None
        last_received: float | None = None
        terminal_at: float | None = None
        terminal_seq: int | None = None
        rejected: set[int] = set()

        for seq in sorted(self.events_by_seq):
            stored = self.events_by_seq[seq]
            ev = stored.event
            candidate = _EVENT_TO_STATUS[ev.event_type]

            if status in _TERMINAL_STATUS:
                # 终态之后的任何事件都不得回退状态（含过期的非终态事件）
                rejected.add(seq)
                continue
            if status == RunStatus.RUNNING and ev.event_type == EventType.START:
                # 已经运行中，重复 START 属于非法跃迁，忽略但留痕
                rejected.add(seq)
                continue
            if status == RunStatus.PENDING and ev.event_type == EventType.PROGRESS:
                # 尚未 START 的 PROGRESS 无法应用，忽略但留痕
                rejected.add(seq)
                continue

            status = candidate
            applied_seq = seq
            if ev.event_type == EventType.PROGRESS and ev.progress is not None:
                progress = ev.progress
            if ev.event_type == EventType.SUCCEEDED:
                progress = 100
            last_occurred = ev.occurred_at
            last_received = stored.received_at
            if status in _TERMINAL_STATUS:
                terminal_at = stored.received_at
                terminal_seq = seq

        return _ReplayResult(
            status=status,
            applied_seq=applied_seq,
            progress=progress,
            last_occurred_at=last_occurred,
            last_received_at=last_received,
            terminal_at=terminal_at,
            rejected_sequences=rejected,
            terminal_seq=terminal_seq,
        )

    def snapshot(self) -> RunStateView:
        r = self.replay()
        return RunStateView(
            run_id=self.run_id,
            task_id=self.task_id,
            status=r.status,
            high_watermark=self.high_watermark,
            progress=r.progress,
            last_occurred_at=r.last_occurred_at,
            last_received_at=r.last_received_at,
            terminal_at=r.terminal_at,
            event_count=len(self.events_by_seq),
        )


class RunStore:
    def __init__(self, clock: Clock):
        self._clock = clock
        self._runs: dict[str, RunAggregate] = {}
        self._lock = threading.RLock()

    def ingest(self, event: RunEvent) -> tuple[RunAggregate, AcceptReason]:
        """摄入事件；调用方需保证同一 run 的摄入串行化（此处仍加锁兜底）。"""
        with self._lock:
            agg = self._runs.get(event.run_id)
            if agg is None:
                agg = RunAggregate(run_id=event.run_id, task_id=event.task_id)
                self._runs[event.run_id] = agg
            reason = agg.ingest(event, self._clock.now())
            return agg, reason

    def get(self, run_id: str) -> RunAggregate | None:
        with self._lock:
            return self._runs.get(run_id)

    def list_runs(self, task_id: str | None = None) -> list[RunStateView]:
        with self._lock:
            aggs = list(self._runs.values())
        if task_id is not None:
            aggs = [a for a in aggs if a.task_id == task_id]
        return [a.snapshot() for a in aggs]
