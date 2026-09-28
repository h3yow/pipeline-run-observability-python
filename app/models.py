"""事件模型与领域枚举（类型定义层）。"""
from __future__ import annotations

from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class EventType(str, Enum):
    STARTED = "STARTED"
    PROGRESS = "PROGRESS"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELED = "CANCELED"


class RunState(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELED = "CANCELED"


class RejectReason(str, Enum):
    DUPLICATE_EVENT = "DUPLICATE_EVENT"
    OUT_OF_ORDER = "OUT_OF_ORDER"
    TERMINAL_STALE = "TERMINAL_STALE"
    INVALID_PAYLOAD = "INVALID_PAYLOAD"
    CLOCK_SKEW = "CLOCK_SKEW"
    RUN_NOT_FOUND = "RUN_NOT_FOUND"
    TASK_NOT_FOUND = "TASK_NOT_FOUND"
    INTERNAL_ERROR = "INTERNAL_ERROR"


class IncidentType(str, Enum):
    MISSED_RUN = "MISSED_RUN"
    STALE_DATA = "STALE_DATA"


class IncidentStatus(str, Enum):
    FIRING = "FIRING"
    RESOLVED = "RESOLVED"


# 事件类型 -> 其发生后运行所处的状态
EVENT_STATE: dict[EventType, RunState] = {
    EventType.STARTED: RunState.RUNNING,
    EventType.PROGRESS: RunState.RUNNING,
    EventType.SUCCEEDED: RunState.SUCCEEDED,
    EventType.FAILED: RunState.FAILED,
    EventType.CANCELED: RunState.CANCELED,
}

# 同 (occurred_at, seq) 并列时的确定性优先级（值越大越优先）
EVENT_RANK: dict[EventType, int] = {
    EventType.STARTED: 0,
    EventType.PROGRESS: 1,
    EventType.SUCCEEDED: 2,
    EventType.FAILED: 2,
    EventType.CANCELED: 2,
}

TERMINAL_STATES = frozenset({RunState.SUCCEEDED, RunState.FAILED, RunState.CANCELED})


class RunEvent(BaseModel):
    run_id: str = Field(min_length=1)
    task_id: str = Field(min_length=1)
    event_type: EventType
    occurred_at: float = Field(ge=0)
    progress: Optional[float] = Field(default=None, ge=0, le=100)
    seq: int = Field(default=0, ge=0)
    client_id: str = Field(min_length=1)


class EventDecision(BaseModel):
    accepted: bool
    run_id: str
    current_state: RunState
    reject_reason: Optional[RejectReason] = None
    detail: str = ""


class RunSnapshot(BaseModel):
    run_id: str
    task_id: str
    state: RunState
    last_event_type: Optional[EventType] = None
    last_occurred_at: Optional[float] = None
    progress: Optional[float] = None
    event_count: int = 0


class TaskConfig(BaseModel):
    task_id: str = Field(min_length=1)
    expected_period_seconds: float = Field(gt=0)
    grace_seconds: float = Field(default=0.0, ge=0)
    miss_after_periods: float = Field(default=1.0, gt=0)
    staleness_multiplier: float = Field(default=2.0, gt=0)


class FreshnessStatus(str, Enum):
    FRESH = "FRESH"
    OVERDUE = "OVERDUE"
    MISSED = "MISSED"
    NO_DATA = "NO_DATA"


class FreshnessReport(BaseModel):
    task_id: str
    status: FreshnessStatus
    expected_period_seconds: float
    lag_seconds: Optional[float] = None
    last_seen_at: Optional[float] = None
    evaluated_at: float
    reason: str = ""


class Incident(BaseModel):
    incident_id: str
    task_id: str
    incident_type: IncidentType
    status: IncidentStatus
    opened_at: float
    resolved_at: Optional[float] = None
    detail: str = ""


class ErrorBody(BaseModel):
    error: RejectReason
    message: str
