"""HTTP 路由层：只做协议适配，领域逻辑全部在 service 中。"""

from __future__ import annotations

from fastapi import APIRouter, Request

from .models import (
    AlertState,
    AlertView,
    EventIngestResult,
    FreshnessView,
    RunEventIn,
    RunStateView,
    TaskSpec,
    TaskSpecIn,
)

router = APIRouter()


def get_service(request: Request):
    return request.app.state.service


@router.post("/tasks", response_model=TaskSpec, status_code=201)
def register_task(payload: TaskSpecIn, request: Request) -> TaskSpec:
    return get_service(request).register_task(payload)


@router.post("/runs/events", response_model=EventIngestResult, status_code=200)
def ingest_event(payload: RunEventIn, request: Request) -> EventIngestResult:
    return get_service(request).ingest_event(payload)


@router.get("/runs/{run_id}", response_model=RunStateView)
def get_run(run_id: str, request: Request) -> RunStateView:
    return get_service(request).get_run(run_id)


@router.get("/runs", response_model=list[RunStateView])
def list_runs(request: Request, task_id: str | None = None) -> list[RunStateView]:
    return get_service(request).list_runs(task_id)


@router.get("/tasks/{task_id}/freshness", response_model=FreshnessView)
def task_freshness(task_id: str, request: Request) -> FreshnessView:
    return get_service(request).evaluate_freshness(task_id)


@router.get("/freshness", response_model=list[FreshnessView])
def list_freshness(request: Request) -> list[FreshnessView]:
    return get_service(request).list_freshness()


@router.get("/alerts", response_model=list[AlertView])
def list_alerts(request: Request, state: AlertState | None = None) -> list[AlertView]:
    return get_service(request).list_alerts(state)
