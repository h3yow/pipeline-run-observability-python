"""HTTP 接口层。

- POST /runs/events              上报事件（重复/乱序/终态过期均以 200 + accepted=false 返回，
                                  携带可区分的 reject_reason）
- GET  /runs/{run_id}            查询运行收敛快照
- POST /tasks                    注册任务及其期望周期
- GET  /tasks/{id}/freshness     查询新鲜度结论（并推进告警状态机）
- GET  /incidents                查询告警（可按 task_id 过滤）
"""
from __future__ import annotations

from fastapi import APIRouter, Request

from app.models import EventDecision, RunEvent, RunSnapshot, TaskConfig
from app.service import ObservabilityService

router = APIRouter()


def get_service(request: Request) -> ObservabilityService:
    return request.app.state.service


@router.post("/runs/events", response_model=EventDecision)
def post_event(event: RunEvent, request: Request) -> EventDecision:
    return get_service(request).ingest_event(event)


@router.get("/runs/{run_id}", response_model=RunSnapshot)
def get_run(run_id: str, request: Request) -> RunSnapshot:
    return get_service(request).get_run(run_id)


@router.post("/tasks", response_model=TaskConfig)
def post_task(config: TaskConfig, request: Request) -> TaskConfig:
    return get_service(request).register_task(config)


@router.get("/tasks/{task_id}/freshness")
def get_freshness(task_id: str, request: Request):
    return get_service(request).check_task(task_id)


@router.get("/incidents")
def get_incidents(request: Request, task_id: str | None = None):
    return get_service(request).list_incidents(task_id)
