"""应用服务编排：事件摄入、状态收敛、新鲜度评估、告警联动。"""

from __future__ import annotations

import logging
import threading

from .alerts import AlertManager
from .clock import Clock
from .errors import (
    DuplicateEventError,
    InvalidEventError,
    RunAlreadyTerminalError,
    StaleEventError,
    UnknownRunError,
    UnknownTaskError,
)
from .freshness import FreshnessService
from .models import (
    AlertState,
    AlertView,
    EventIngestResult,
    EventType,
    FaultType,
    FreshnessState,
    FreshnessView,
    RunEventIn,
    RunStateView,
    TaskSpec,
    TaskSpecIn,
)
from .run_store import RunStore
from .tasks import TaskRegistry

logger = logging.getLogger("pipeline-observability.service")

# 允许的生产者时钟相对服务端的最大前漂（秒），超出视为不可信事件
MAX_FUTURE_SKEW_SECONDS = 60.0


class ObservabilityService:
    def __init__(self, clock: Clock | None = None) -> None:
        self._clock = clock or Clock()
        self._tasks = TaskRegistry()
        self._runs = RunStore(self._clock)
        self._freshness = FreshnessService(self._clock, self._tasks)
        self._alerts = AlertManager(self._clock)
        # 串行化“摄入 + 收敛 + 告警联动”，保证并发写入下结论一致
        self._ingest_lock = threading.RLock()

    # ------------------------------------------------------------------ tasks
    def register_task(self, payload: TaskSpecIn) -> TaskSpec:
        spec = TaskSpec(**payload.model_dump())
        return self._tasks.register(spec)

    # ----------------------------------------------------------------- events
    def ingest_event(self, payload: RunEventIn) -> EventIngestResult:
        with self._ingest_lock:
            return self._ingest_locked(payload)

    def _validate_event(self, event: RunEventIn) -> None:
        spec = self._tasks.get(event.task_id)
        if spec is None:
            raise UnknownTaskError(detail={"task_id": event.task_id})

        now = self._clock.now()
        if event.occurred_at > now + MAX_FUTURE_SKEW_SECONDS:
            raise InvalidEventError(
                "event occurred_at is too far in the future",
                detail={
                    "event_id": event.event_id,
                    "occurred_at": event.occurred_at,
                    "server_now": now,
                },
            )
        if event.event_type == EventType.PROGRESS and event.progress is None:
            raise InvalidEventError(
                "PROGRESS event requires progress",
                detail={"event_id": event.event_id},
            )
        if event.event_type != EventType.PROGRESS and event.progress is not None:
            raise InvalidEventError(
                "progress is only allowed on PROGRESS events",
                detail={"event_id": event.event_id},
            )
        if event.event_type == EventType.FAILED and not event.error:
            raise InvalidEventError(
                "FAILED event requires error",
                detail={"event_id": event.event_id},
            )

    def _ingest_locked(self, payload: RunEventIn) -> EventIngestResult:
        self._validate_event(payload)

        agg, reason = self._runs.ingest(payload)
        after = agg.snapshot()
        received_at = self._clock.now()

        logger.info(
            "ingest event_id=%s run_id=%s task_id=%s type=%s seq=%s occurred_at=%.3f "
            "decision=%s resulting_status=%s high_watermark=%s",
            payload.event_id, payload.run_id, payload.task_id,
            payload.event_type.value, payload.seq, payload.occurred_at,
            reason.value, after.status.value, after.high_watermark,
        )

        if reason.value == "DUPLICATE":
            raise DuplicateEventError(
                detail={"event_id": payload.event_id, "run_id": payload.run_id}
            )
        if reason.value == "STALE":
            raise StaleEventError(
                detail={
                    "event_id": payload.event_id,
                    "run_id": payload.run_id,
                    "seq": payload.seq,
                }
            )

        # 事件被保留：判断它在重放中是否被忽略以及具体原因
        replayed = agg.replay()
        ignored = payload.seq in replayed.rejected_sequences

        if ignored and replayed.terminal_seq is not None and payload.seq > replayed.terminal_seq:
            # 运行已终态且该事件序号更晚：过期事件不得回退终态
            raise RunAlreadyTerminalError(
                detail={
                    "event_id": payload.event_id,
                    "run_id": payload.run_id,
                    "terminal_status": replayed.status.value,
                    "terminal_seq": replayed.terminal_seq,
                    "event_seq": payload.seq,
                }
            )
        if ignored:
            # 终态之前但跃迁非法（孤立 PROGRESS、运行中重复 START）：保留但不可应用
            logger.info(
                "event retained but not applied event_id=%s run_id=%s reason=INVALID_TRANSITION",
                payload.event_id, payload.run_id,
            )

        # 新鲜度与告警联动（事件只要被保留即视为一次有效心跳）
        self._freshness.observe(payload.task_id, payload.occurred_at, received_at)
        freshness = self._freshness.evaluate(payload.task_id)
        self._sync_stalled_alert(payload.task_id, freshness)

        return EventIngestResult(
            accepted=True,
            reason="ACCEPTED",
            run_id=payload.run_id,
            status=after.status,
            applied_seq=replayed.applied_seq,
            detail={
                "ignored_by_convergence": ignored,
                "freshness": freshness.state.value,
                "lag_seconds": freshness.lag_seconds,
            },
        )

    def _sync_stalled_alert(self, task_id: str, freshness: FreshnessView) -> None:
        if freshness.state == FreshnessState.OVERDUE:
            _, fired = self._alerts.report_fault(
                task_id, FaultType.STALLED, freshness.reason
            )
            if fired:
                logger.warning("ALERT FIRED task=%s %s", task_id, freshness.reason)
        else:
            self._alerts.report_recovery(
                task_id, FaultType.STALLED, freshness.reason
            )

    def _evaluate_and_sync(self, task_id: str) -> FreshnessView:
        """评估单个任务新鲜度，并把结论同步到告警（覆盖仅时间流逝导致的超期）。"""
        freshness = self._freshness.evaluate(task_id)
        self._sync_stalled_alert(task_id, freshness)
        return freshness

    # ------------------------------------------------------------------ query
    def get_run(self, run_id: str) -> RunStateView:
        agg = self._runs.get(run_id)
        if agg is None:
            raise UnknownRunError(detail={"run_id": run_id})
        return agg.snapshot()

    def list_runs(self, task_id: str | None = None) -> list[RunStateView]:
        return self._runs.list_runs(task_id)

    def evaluate_freshness(self, task_id: str) -> FreshnessView:
        with self._ingest_lock:
            return self._evaluate_and_sync(task_id)

    def list_freshness(self) -> list[FreshnessView]:
        with self._ingest_lock:
            views = self._freshness.evaluate_all()
            for view in views:
                self._sync_stalled_alert(view.task_id, view)
            return views

    def list_alerts(self, state: AlertState | None = None) -> list[AlertView]:
        return self._alerts.list_alerts(state)
