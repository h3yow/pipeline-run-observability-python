"""新鲜度判定（纯函数式规则，结论对同一数据稳定）。

判定依据：任务自身的期望周期 expected_period_seconds 与宽限 grace_seconds。
设 elapsed = max(0, evaluated_at - last_seen_at)（钳制负数以对抗时钟回拨）：

- last_seen_at 为 None（从未上报）          -> NO_DATA
- elapsed <= grace                         -> FRESH
- grace < elapsed < period * miss_after    -> OVERDUE（给出滞后时长）
- elapsed >= period * miss_after           -> MISSED

滞后时长与结论只取决于 (last_seen_at, evaluated_at)，同一数据反复评估结论不变，
不会因为一次瞬时抖动反复翻转。
"""
from __future__ import annotations

from app.models import FreshnessReport, FreshnessStatus, TaskConfig


def _round(value: float) -> float:
    return round(value, 6)


def evaluate_freshness(
    config: TaskConfig,
    last_seen_at: float | None,
    evaluated_at: float,
) -> FreshnessReport:
    period = config.expected_period_seconds
    grace = config.grace_seconds
    miss_threshold = period * config.miss_after_periods

    # 首次上报前：明确且稳定的 NO_DATA，不制造误报
    if last_seen_at is None:
        return FreshnessReport(
            task_id=config.task_id,
            status=FreshnessStatus.NO_DATA,
            expected_period_seconds=period,
            lag_seconds=None,
            last_seen_at=None,
            evaluated_at=evaluated_at,
            reason="任务从未上报数据",
        )

    # 时钟回拨（evaluated_at < last_seen_at）时滞后钳为 0，视为 FRESH，
    # 绝不因为回拨产生“凭空滞后”
    elapsed = evaluated_at - last_seen_at
    if elapsed < 0:
        elapsed = 0.0
    lag = max(0.0, elapsed - grace)

    if elapsed >= miss_threshold:
        status = FreshnessStatus.MISSED
        reason = (
            f"已 {_round(elapsed)}s 未收到数据，"
            f"超过错过阈值 {_round(miss_threshold)}s"
        )
    elif elapsed > grace:
        status = FreshnessStatus.OVERDUE
        reason = (
            f"滞后 {_round(lag)}s（无数据 {_round(elapsed)}s，"
            f"宽限 {_round(grace)}s）"
        )
    else:
        status = FreshnessStatus.FRESH
        reason = f"最近 {_round(elapsed)}s 内有数据，处于宽限期 {_round(grace)}s 内"

    return FreshnessReport(
        task_id=config.task_id,
        status=status,
        expected_period_seconds=period,
        lag_seconds=_round(lag),
        last_seen_at=last_seen_at,
        evaluated_at=evaluated_at,
        reason=reason,
    )
