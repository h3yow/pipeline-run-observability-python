"""编排层：事件入库、任务注册、新鲜度评估与告警联动。"""
from __future__ import annotations

import logging
import threading

from app.alerts import AlertManager
from app.clock import Clock, SystemClock
from app.errors import (
    ClockSkewError,
    RunNotFoundError,
    TaskNotFoundError,
)
from app.freshness import evaluate_freshness
from app.models import (
    EventDecision,
    FreshnessReport,
    Incident,
    RunEvent,
    RunSnapshot,
    TaskConfig,
)
from app.store import RunStore

logger = logging.getLogger("observability.events")

# 允许客户端事件时间领先服务器单调时钟的上限；超过即判定时钟异常
CLOCK_TOLERANCE_SECONDS = 60.0


class ObservabilityService:
    def __init__(self, clock: Clock | None = None):
        self._clock = clock or SystemClock()
        self._store = RunStore(self._clock)
        self._alerts = AlertManager(self._clock)
        self._tasks: dict[str, TaskConfig] = {}
        self._last_seen: dict[str, float] = {}
        self._lock = threading.RLock()

    def register_task(self, config: TaskConfig) -> TaskConfig:
        with self._lock:
            self._tasks[config.task_id] = config
            self._last_seen.setdefault(config.task_id, None)
            logger.info("register_task task_id=%s config=%s", config.task_id, config)
            return config

    def ingest_event(self, event: RunEvent) -> EventDecision:
        """上报事件：先做时钟异常校验，再交给存储层收敛，最后更新任务最近可见时间。"""
        now = self._clock.now()

        # 时钟异常：事件时间大幅领先服务器时间，判定为时钟回拨/漂移
        if event.occurred_at > now + CLOCK_TOLERANCE_SECONDS:
            logger.warning(
                "event_clock_skew run_id=%s occurred_at=%s server_now=%s",
                event.run_id,
                event.occurred_at,
                now,
            )
            raise ClockSkewError(
                f"事件时间 {event.occurred_at} 领先服务器时间 {now} "
                f"超过容差 {CLOCK_TOLERANCE_SECONDS}s"
            )

        with self._lock:
            decision = self._store.ingest(event)
            # 只有被接收的事件才推进“最近可见时间”（用服务器接收时间），
            # 拒绝的重复/乱序/终态过期事件不影响新鲜度
            if decision.accepted:
                self._last_seen[event.task_id] = now
            logger.info(
                "event_ingest run_id=%s task_id=%s type=%s occurred_at=%s seq=%s "
                "-> accepted=%s state=%s reason=%s detail=%s",
                event.run_id,
                event.task_id,
                event.event_type.value,
                event.occurred_at,
                event.seq,
                decision.accepted,
                decision.current_state.value,
                decision.reject_reason.value if decision.reject_reason else "ACCEPTED",
                decision.detail,
            )
            return decision

    def get_run(self, run_id: str) -> RunSnapshot:
        snapshot = self._store.get_snapshot(run_id)
        if snapshot is None:
            raise RunNotFoundError(f"运行 {run_id} 不存在")
        return snapshot

    def check_task(self, task_id: str) -> FreshnessReport:
        """评估任务新鲜度，并把结论推进告警状态机。"""
        with self._lock:
            config = self._tasks.get(task_id)
            if config is None:
                raise TaskNotFoundError(f"任务 {task_id} 未注册")
            now = self._clock.now()
            last_seen = self._last_seen.get(task_id)
            report = evaluate_freshness(config, last_seen, now)
            transitions = self._alerts.evaluate(report)
            for incident in transitions:
                logger.info(
                    "incident_transition task_id=%s type=%s status=%s reason=%s",
                    task_id,
                    incident.incident_type.value,
                    incident.status.value,
                    incident.detail,
                )
            return report

    def list_incidents(self, task_id: str | None = None) -> list[Incident]:
        return self._alerts.list_incidents(task_id)

    def reset(self) -> None:
        with self._lock:
            self._store.reset()
            self._alerts.reset()
            self._tasks.clear()
            self._last_seen.clear()
