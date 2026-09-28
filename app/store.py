"""运行事件存储：加锁、按序收敛，保证并发一致。

收敛规则（与到达顺序无关）：
- 事件顺序键 = (occurred_at, seq, 事件优先级)，同键不同事件按确定性优先级比较；
- 事件身份键 = (event_type, occurred_at, seq)，重复投递判定为 DUPLICATE_EVENT；
- 运行当前状态 = 顺序键最大的已接收事件对应状态；
- 一旦顺序键最大的事件是终态（SUCCEEDED/FAILED/CANCELED），
  任何其他事件都不能再改变它（TERMINAL_STALE）；
- 晚到、键更小的事件被拒绝（OUT_OF_ORDER），且不影响当前状态。
"""
from __future__ import annotations

import threading
from dataclasses import dataclass, field

from app.clock import Clock, SystemClock
from app.models import (
    EVENT_RANK,
    EVENT_STATE,
    EventDecision,
    EventType,
    RejectReason,
    RunEvent,
    RunSnapshot,
    RunState,
)

OrderKey = tuple[float, int, int]


def _order_key(event: RunEvent) -> OrderKey:
    return (event.occurred_at, event.seq, EVENT_RANK[event.event_type])


def _identity(event: RunEvent) -> tuple[EventType, float, int]:
    return (event.event_type, event.occurred_at, event.seq)


@dataclass
class _Run:
    run_id: str
    task_id: str
    events: dict[tuple[EventType, float, int], RunEvent] = field(default_factory=dict)
    winner_key: OrderKey | None = None
    winner_type: EventType | None = None
    state: RunState = RunState.PENDING
    progress: float | None = None


class RunStore:
    def __init__(self, clock: Clock | None = None):
        self._clock = clock or SystemClock()
        self._runs: dict[str, _Run] = {}
        self._lock = threading.RLock()

    # ---- 内部工具（调用方须持锁） ----
    def _progress_of(self, run: _Run) -> float | None:
        best: tuple[OrderKey, float] | None = None
        for event in run.events.values():
            if event.progress is None:
                continue
            key = _order_key(event)
            if best is None or key > best[0]:
                best = (key, event.progress)
        return best[1] if best is not None else None

    def ingest(self, event: RunEvent) -> EventDecision:
        """接收一个事件，返回判定结果（accepted/rejected + 当前状态 + 依据）。"""
        with self._lock:
            run = self._runs.get(event.run_id)
            if run is None:
                run = _Run(run_id=event.run_id, task_id=event.task_id)
                self._runs[event.run_id] = run

            key = _order_key(event)
            identity = _identity(event)

            # 1) 完全相同的事件身份 -> 幂等重复
            if identity in run.events:
                return EventDecision(
                    accepted=False,
                    run_id=event.run_id,
                    current_state=run.state,
                    reject_reason=RejectReason.DUPLICATE_EVENT,
                    detail=(
                        f"事件 {event.event_type.value}@(occurred_at="
                        f"{event.occurred_at}, seq={event.seq}) 已接收，"
                        "重复投递被忽略"
                    ),
                )

            # 2) 已终结：终态是吸收态，任何事件不得回退或覆盖
            if run.state in (
                RunState.SUCCEEDED,
                RunState.FAILED,
                RunState.CANCELED,
            ):
                return EventDecision(
                    accepted=False,
                    run_id=event.run_id,
                    current_state=run.state,
                    reject_reason=RejectReason.TERMINAL_STALE,
                    detail=(
                        f"运行已处于终态 {run.state.value}，"
                        f"事件 {event.event_type.value} 被拒绝"
                    ),
                )

            # 3) 非终态下的乱序/并列冲突：新事件必须严格新于当前获胜事件
            if run.winner_key is not None and key <= run.winner_key:
                return EventDecision(
                    accepted=False,
                    run_id=event.run_id,
                    current_state=run.state,
                    reject_reason=RejectReason.OUT_OF_ORDER,
                    detail=(
                        f"事件顺序键 {key} 不新于已接收最新键 {run.winner_key}，"
                        "乱序/并列冲突被拒绝"
                    ),
                )

            # 4) 接收并收敛
            run.events[identity] = event
            run.winner_key = key
            run.winner_type = event.event_type
            run.state = EVENT_STATE[event.event_type]
            if event.progress is not None:
                run.progress = event.progress
            else:
                run.progress = self._progress_of(run)

            return EventDecision(
                accepted=True,
                run_id=event.run_id,
                current_state=run.state,
                detail=f"事件已接收，收敛后状态={run.state.value}",
            )

    def get_snapshot(self, run_id: str) -> RunSnapshot | None:
        with self._lock:
            run = self._runs.get(run_id)
            if run is None:
                return None
            return RunSnapshot(
                run_id=run.run_id,
                task_id=run.task_id,
                state=run.state,
                last_event_type=run.winner_type,
                last_occurred_at=run.winner_key[0] if run.winner_key else None,
                progress=run.progress,
                event_count=len(run.events),
            )

    def reset(self) -> None:
        with self._lock:
            self._runs.clear()
