"""测试夹具与公共日志：每个用例都打印事件输入与最终判定依据。"""
from __future__ import annotations

import logging
import sys

import pytest
from fastapi.testclient import TestClient

from app.clock import ManualClock
from app.main import create_app
from app.service import ObservabilityService

# 让领域日志在用例中可见（配合 pytest log_cli / -s）
logging.basicConfig(
    level=logging.INFO,
    stream=sys.stdout,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

case_logger = logging.getLogger("observability.tests")


@pytest.fixture
def clock() -> ManualClock:
    return ManualClock(start=1000.0)


@pytest.fixture
def service(clock: ManualClock) -> ObservabilityService:
    svc = ObservabilityService(clock)
    yield svc
    svc.reset()


@pytest.fixture
def client(service: ObservabilityService) -> TestClient:
    app = create_app(service)
    return TestClient(app, raise_server_exceptions=False)


def log_decision(where: str, event, decision) -> None:
    """统一打印：事件输入 + 最终判定依据。"""
    case_logger.info(
        "[%s] 输入事件 run_id=%s task_id=%s type=%s occurred_at=%s seq=%s "
        "progress=%s client_id=%s => 判定 accepted=%s state=%s reason=%s | %s",
        where,
        event.run_id,
        event.task_id,
        event.event_type.value,
        event.occurred_at,
        event.seq,
        event.progress,
        event.client_id,
        decision.accepted,
        decision.current_state.value,
        decision.reject_reason.value if decision.reject_reason else "ACCEPTED",
        decision.detail,
    )
