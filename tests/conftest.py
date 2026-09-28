"""测试公共夹具：可控时钟 + TestClient。"""

from __future__ import annotations

import logging

import pytest
from fastapi.testclient import TestClient

import main
from app.clock import Clock
from app.service import ObservabilityService

logging.basicConfig(level=logging.INFO)


class MutableTime:
    def __init__(self, start: float = 1000.0) -> None:
        self.value = start

    def __call__(self) -> float:
        return self.value


@pytest.fixture
def mutable_time() -> MutableTime:
    return MutableTime()


@pytest.fixture
def service(mutable_time: MutableTime) -> ObservabilityService:
    return ObservabilityService(clock=Clock(mutable_time))


@pytest.fixture
def client(service: ObservabilityService) -> TestClient:
    main.app.state.service = service
    return TestClient(main.app)
