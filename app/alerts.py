"""告警去重与恢复：同一故障持续期只告警一次，恢复后再次故障重新触发。"""

from __future__ import annotations

import threading
from dataclasses import dataclass

from .clock import Clock
from .models import AlertState, AlertView, FaultType


@dataclass
class Alert:
    key: str
    task_id: str
    fault_type: FaultType
    state: AlertState
    first_seen_at: float
    last_seen_at: float
    resolved_at: float | None = None
    count: int = 0
    reason: str = ""

    def view(self) -> AlertView:
        return AlertView(
            key=self.key,
            task_id=self.task_id,
            fault_type=self.fault_type,
            state=self.state,
            first_seen_at=self.first_seen_at,
            last_seen_at=self.last_seen_at,
            resolved_at=self.resolved_at,
            count=self.count,
            reason=self.reason,
        )


class AlertManager:
    """
    去重语义（key = task_id + fault_type）：
    - FIRING 期间重复 report_fault：只更新 last_seen/count，fired=False，不产生新告警；
    - report_recovery：FIRING -> RESOLVED；已恢复则为空操作；
    - RESOLVED 之后再次 report_fault：重新打开为 FIRING，fired=True（重新触发）。
    所有变更在锁内完成，并发上报不会产生重复告警。
    """

    def __init__(self, clock: Clock) -> None:
        self._clock = clock
        self._alerts: dict[str, Alert] = {}
        self._lock = threading.RLock()

    @staticmethod
    def make_key(task_id: str, fault_type: FaultType) -> str:
        return f"{task_id}:{fault_type.value}"

    def report_fault(self, task_id: str, fault_type: FaultType, reason: str) -> tuple[Alert, bool]:
        """记录一次故障；返回 (告警, 是否为新触发)。"""
        key = self.make_key(task_id, fault_type)
        now = self._clock.now()
        with self._lock:
            alert = self._alerts.get(key)
            if alert is None:
                alert = Alert(
                    key=key,
                    task_id=task_id,
                    fault_type=fault_type,
                    state=AlertState.FIRING,
                    first_seen_at=now,
                    last_seen_at=now,
                    count=1,
                    reason=reason,
                )
                self._alerts[key] = alert
                return alert, True

            alert.last_seen_at = now
            alert.reason = reason
            if alert.state == AlertState.FIRING:
                # 持续期内重复故障：去重，仅计数
                alert.count += 1
                return alert, False

            # 此前已恢复：重新触发
            alert.state = AlertState.FIRING
            alert.first_seen_at = now
            alert.resolved_at = None
            alert.count = 1
            return alert, True

    def report_recovery(self, task_id: str, fault_type: FaultType, reason: str) -> Alert | None:
        key = self.make_key(task_id, fault_type)
        now = self._clock.now()
        with self._lock:
            alert = self._alerts.get(key)
            if alert is None or alert.state == AlertState.RESOLVED:
                return alert
            alert.state = AlertState.RESOLVED
            alert.resolved_at = now
            alert.reason = reason
            return alert

    def list_alerts(self, state: AlertState | None = None) -> list[AlertView]:
        with self._lock:
            alerts = list(self._alerts.values())
        if state is not None:
            alerts = [a for a in alerts if a.state == state]
        return [a.view() for a in alerts]
