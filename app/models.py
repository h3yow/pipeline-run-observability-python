"""事件模型与对外视图的类型定义。"""

from __future__ import annotations

from enum import Enum

from pydantic import BaseModel, Field


class EventType(str, Enum):
    START = "START"
    PROGRESS = "PROGRESS"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


TERMINAL_EVENT_TYPES = frozenset({EventType.SUCCEEDED, EventType.FAILED, EventType.CANCELLED})


class RunStatus(str, Enum):
    # 尚未收到可应用的起始事件（例如存在序号缺口）
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"

    @property
    def is_terminal(self) -> bool:
        return self in TERMINAL_STATUSES


TERMINAL_STATUSES = frozenset(
    {RunStatus.SUCCEEDED, RunStatus.FAILED, RunStatus.CANCELLED}
)


class FreshnessState(str, Enum):
    # 从未收到过事件
    NO_DATA = "NO_DATA"
    FRESH = "FRESH"
    OVERDUE = "OVERDUE"


class AlertState(str, Enum):
    FIRING = "FIRING"
    RESOLVED = "RESOLVED"


class FaultType(str, Enum):
    STALLED = "STALLED"


class AcceptReason(str, Enum):
    ACCEPTED = "ACCEPTED"
    DUPLICATE = "DUPLICATE"
    STALE = "STALE"
    ALREADY_TERMINAL = "ALREADY_TERMINAL"
    INVALID_TRANSITION = "INVALID_TRANSITION"


class RunEvent(BaseModel):
    event_id: str = Field(min_length=1)
    run_id: str = Field(min_length=1)
    task_id: str = Field(min_length=1)
    event_type: EventType
    # 生产者维护的单调递增序号，用于乱序排序与高水位判断
    seq: int = Field(ge=0)
    # 生产者时钟下的事件发生时间（epoch 秒）
    occurred_at: float
    # 进度百分比，仅 PROGRESS 使用
    progress: int | None = Field(default=None, ge=0, le=100)
    # 失败原因，仅 FAILED 使用
    error: str | None = None


class RunEventIn(RunEvent):
    """入站事件（与 RunEvent 相同，便于未来区分服务端填充字段）。"""


class RunEventOut(BaseModel):
    event_id: str
    run_id: str
    task_id: str
    event_type: EventType
    seq: int
    occurred_at: float
    progress: int | None
    error: str | None
    received_at: float


class EventIngestResult(BaseModel):
    accepted: bool
    reason: AcceptReason
    run_id: str
    status: RunStatus
    applied_seq: int | None = None
    detail: dict = Field(default_factory=dict)


class RunStateView(BaseModel):
    run_id: str
    task_id: str
    status: RunStatus
    high_watermark: int
    progress: int | None
    last_occurred_at: float | None
    last_received_at: float | None
    terminal_at: float | None
    event_count: int


class TaskSpecIn(BaseModel):
    task_id: str = Field(min_length=1)
    expected_period_seconds: float = Field(gt=0)
    lag_tolerance_seconds: float = Field(default=0.0, ge=0)
    recovery_grace_seconds: float = Field(default=0.0, ge=0)


class TaskSpec(BaseModel):
    task_id: str
    expected_period_seconds: float
    lag_tolerance_seconds: float = 0.0
    recovery_grace_seconds: float = 0.0


class FreshnessView(BaseModel):
    task_id: str
    state: FreshnessState
    lag_seconds: float | None
    expected_period_seconds: float
    lag_tolerance_seconds: float
    recovery_grace_seconds: float
    last_event_at: float | None
    observed_at: float
    clock_skew: bool
    reason: str


class AlertView(BaseModel):
    key: str
    task_id: str
    fault_type: FaultType
    state: AlertState
    first_seen_at: float
    last_seen_at: float
    resolved_at: float | None
    count: int
    reason: str


class ErrorBody(BaseModel):
    error_code: str
    message: str
    detail: dict = Field(default_factory=dict)
