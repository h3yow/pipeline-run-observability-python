"""告警去重与恢复：同一故障持续期只告警一次，恢复后重新触发。

状态机（按 (task_id, incident_type) 维度，每个维度只保留一条活动告警）：

- 健康（FRESH/NO_DATA）且有 FIRING 告警 -> 关闭（RESOLVED）；
- OVERDUE/MISSED 且无 FIRING 告警      -> 新开一条 FIRING；
- OVERDUE/MISSED 且已有 FIRING 告警    -> 不重复告警（幂等）；
- 健康且无告警                         -> 无动作。

并发上报通过同一把锁串行化“评估+转态”，保证不会产生重复告警。
历史（含已恢复）告警保留在记录中，供查询与测试核对。
"""
from __future__ import annotations

import threading

from app.clock import Clock, SystemClock
from app.models import (
    FreshnessReport,
    FreshnessStatus,
    Incident,
    IncidentStatus,
    IncidentType,
)

# 新鲜度结论 -> 对应故障类型；FRESH / NO_DATA 视为健康（None）
_STATUS_TO_INCIDENT: dict[FreshnessStatus, IncidentType | None] = {
    FreshnessStatus.FRESH: None,
    FreshnessStatus.NO_DATA: None,
    FreshnessStatus.OVERDUE: IncidentType.STALE_DATA,
    FreshnessStatus.MISSED: IncidentType.MISSED_RUN,
}


class AlertManager:
    def __init__(self, clock: Clock | None = None):
        self._clock = clock or SystemClock()
        self._lock = threading.RLock()
        # key=(task_id, type) -> 当前活动告警；None 表示无活动告警
        self._active: dict[tuple[str, IncidentType], Incident | None] = {}
        self._history: list[Incident] = []
        self._seq = 0

    def evaluate(self, report: FreshnessReport) -> list[Incident]:
        """根据一次新鲜度评估推进告警状态机，返回本次发生转态的告警列表。"""
        now = self._clock.now()
        desired = _STATUS_TO_INCIDENT[report.status]
        transitions: list[Incident] = []

        with self._lock:
            keys = set(self._active.keys())
            if desired is not None:
                keys.add((report.task_id, desired))

            for key in keys:
                task_id, incident_type = key
                active = self._active.get(key)
                healthy = desired is None or desired != incident_type

                if healthy:
                    # 健康：关闭该类型仍在活动的告警
                    if active is not None and active.status == IncidentStatus.FIRING:
                        active.status = IncidentStatus.RESOLVED
                        active.resolved_at = now
                        active.detail = f"已恢复（评估结论={report.status.value}）"
                        self._active[key] = None
                        transitions.append(active)
                else:
                    # 故障中：仅当没有活动告警时新开，避免重复
                    if active is None:
                        self._seq += 1
                        incident = Incident(
                            incident_id=(
                                f"inc-{task_id}-{incident_type.value}-{self._seq}"
                            ),
                            task_id=task_id,
                            incident_type=incident_type,
                            status=IncidentStatus.FIRING,
                            opened_at=now,
                            detail=report.reason,
                        )
                        self._active[key] = incident
                        self._history.append(incident)
                        transitions.append(incident)
                    # 已有 FIRING：幂等，不产生新告警

        return transitions

    def list_incidents(self, task_id: str | None = None) -> list[Incident]:
        with self._lock:
            incidents = list(self._history)
            for active in self._active.values():
                if active is not None and active not in incidents:
                    incidents.append(active)
        if task_id is not None:
            incidents = [i for i in incidents if i.task_id == task_id]
        return incidents

    def reset(self) -> None:
        with self._lock:
            self._active.clear()
            self._history.clear()
            self._seq = 0
