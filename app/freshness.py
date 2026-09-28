"""新鲜度与滞后判定：按任务期望周期给出稳定、可解释的结论。"""

from __future__ import annotations

import threading

from .clock import Clock
from .errors import UnknownTaskError
from .models import FreshnessState, FreshnessView
from .tasks import TaskRegistry

# 生产者时钟回拨的最小判定阈值（秒），小于该值视为普通抖动
CLOCK_SKEW_TOLERANCE_SECONDS = 1.0


class FreshnessService:
    """
    判定口径（lag 基于服务端接收时间 received_at，避免生产者时钟抖动影响）：

    - 从未收到事件：NO_DATA，lag 为 None，不判超期；
    - 首次事件本身不判超期：收到后建立 FRESH 基线；
    - lag = now - last_received_at；
    - lag 超过 expected_period 即视为破限，但需持续 lag_tolerance 秒才判 OVERDUE，
      破限不足宽限停留 GRACE_WINDOW（对外仍为 FRESH），避免一次抖动反复误报；
    - OVERDUE -> FRESH：lag 回到 expected_period + recovery_grace 以内（迟滞带，
      新事件到达后 lag 立即变小，自然恢复）；
    - 生产者时钟（occurred_at）相对上一次回拨超过 CLOCK_SKEW_TOLERANCE_SECONDS
      时 clock_skew=True，仅标记、不改变滞后结论。
    """

    def __init__(self, clock: Clock, tasks: TaskRegistry) -> None:
        self._clock = clock
        self._tasks = tasks
        self._lock = threading.RLock()
        # task_id -> 判定记忆
        self._breaching_since: dict[str, float] = {}
        self._overdue_since: dict[str, float] = {}
        # 新事件到达时若此前处于 OVERDUE，记录待报告的恢复事件（时间戳）
        self._recovered_at: dict[str, float] = {}
        self._last_occurred: dict[str, float] = {}
        self._last_received: dict[str, float] = {}
        self._clock_skew: dict[str, bool] = {}

    def observe(self, task_id: str, occurred_at: float, received_at: float) -> None:
        """用一次新到达的事件刷新滞后基线。"""
        with self._lock:
            prev_occurred = self._last_occurred.get(task_id)
            if (
                prev_occurred is not None
                and occurred_at < prev_occurred - CLOCK_SKEW_TOLERANCE_SECONDS
            ):
                # 时钟回拨：保留旧的 occurred 基线，仅打标；滞后仍以 received_at 为准
                self._clock_skew[task_id] = True
            else:
                if prev_occurred is not None:
                    # 非回拨的新事件，清除历史回拨标记
                    self._clock_skew[task_id] = False
                self._last_occurred[task_id] = occurred_at

            if task_id in self._overdue_since:
                self._recovered_at[task_id] = received_at
            self._last_received[task_id] = received_at
            self._breaching_since.pop(task_id, None)
            self._overdue_since.pop(task_id, None)

    def _evaluate_locked(self, task_id: str) -> FreshnessView:
        spec = self._tasks.get(task_id)
        if spec is None:
            raise UnknownTaskError(detail={"task_id": task_id})

        now = self._clock.now()
        last_received = self._last_received.get(task_id)
        recovery_limit = spec.expected_period_seconds + spec.recovery_grace_seconds

        if last_received is None:
            return FreshnessView(
                task_id=task_id,
                state=FreshnessState.NO_DATA,
                lag_seconds=None,
                expected_period_seconds=spec.expected_period_seconds,
                lag_tolerance_seconds=spec.lag_tolerance_seconds,
                recovery_grace_seconds=spec.recovery_grace_seconds,
                last_event_at=None,
                observed_at=now,
                clock_skew=False,
                reason="NEVER_OBSERVED: no event has ever been received",
            )

        lag = max(0.0, now - last_received)
        skew = self._clock_skew.get(task_id, False)
        overdue_since = self._overdue_since.get(task_id)

        if overdue_since is not None:
            # 已 OVERDUE：只有 lag 回到恢复阈值内才恢复（迟滞防抖）
            if lag <= recovery_limit:
                self._overdue_since.pop(task_id, None)
                self._breaching_since.pop(task_id, None)
                reason = (
                    f"RECOVERED: lag {lag:.3f}s back within recovery limit "
                    f"{recovery_limit:.3f}s"
                )
                state = FreshnessState.FRESH
            else:
                reason = (
                    f"OVERDUE_PERSISTING: lag {lag:.3f}s beyond recovery limit "
                    f"{recovery_limit:.3f}s, overdue since {overdue_since:.3f}"
                )
                state = FreshnessState.OVERDUE
        elif lag > spec.expected_period_seconds:
            # 破限计时从“本应到达的时刻”开始，评估推进不会推迟该基准
            breaching_since = self._breaching_since.get(task_id)
            if breaching_since is None:
                breaching_since = last_received + spec.expected_period_seconds
                self._breaching_since[task_id] = breaching_since
            overdue_for = now - breaching_since
            if overdue_for >= spec.lag_tolerance_seconds:
                self._overdue_since[task_id] = breaching_since
                reason = (
                    f"OVERDUE_DETECTED: lag {lag:.3f}s beyond expected period "
                    f"{spec.expected_period_seconds:.3f}s for {overdue_for:.3f}s"
                    f" (tolerance {spec.lag_tolerance_seconds:.3f}s)"
                )
                state = FreshnessState.OVERDUE
            else:
                reason = (
                    f"GRACE_WINDOW: lag {lag:.3f}s beyond expected period "
                    f"{spec.expected_period_seconds:.3f}s but within tolerance "
                    f"{spec.lag_tolerance_seconds:.3f}s ({overdue_for:.3f}s elapsed)"
                )
                state = FreshnessState.FRESH
        else:
            # 周期内：清除可能存在的破限计时
            self._breaching_since.pop(task_id, None)
            recovered_at = self._recovered_at.pop(task_id, None)
            if recovered_at is not None:
                reason = (
                    f"RECOVERED: fresh event received at {recovered_at:.3f}, "
                    f"lag {lag:.3f}s within recovery limit {recovery_limit:.3f}s"
                )
            else:
                reason = (
                    f"FRESH: lag {lag:.3f}s within expected period "
                    f"{spec.expected_period_seconds:.3f}s"
                )
            state = FreshnessState.FRESH

        return FreshnessView(
            task_id=task_id,
            state=state,
            lag_seconds=round(lag, 6),
            expected_period_seconds=spec.expected_period_seconds,
            lag_tolerance_seconds=spec.lag_tolerance_seconds,
            recovery_grace_seconds=spec.recovery_grace_seconds,
            last_event_at=last_received,
            observed_at=now,
            clock_skew=skew,
            reason=reason,
        )

    def evaluate(self, task_id: str) -> FreshnessView:
        with self._lock:
            return self._evaluate_locked(task_id)

    def evaluate_all(self) -> list[FreshnessView]:
        with self._lock:
            return [self._evaluate_locked(spec.task_id) for spec in self._tasks.list_all()]
