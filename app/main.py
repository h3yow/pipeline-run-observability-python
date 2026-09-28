"""FastAPI 应用入口。"""
from __future__ import annotations

from fastapi import FastAPI

from app.api import router
from app.errors import register_error_handlers
from app.service import ObservabilityService


def create_app(service: ObservabilityService | None = None) -> FastAPI:
    application = FastAPI(title="pipeline-run-observability")
    application.state.service = service or ObservabilityService()
    register_error_handlers(application)
    application.include_router(router)
    return application


app = create_app()
