"""统一错误归一化层：所有对外错误均为可区分的领域错误类型。

- 领域错误 (DomainError) -> 其携带的归一化类型 + 稳定文案；
- 请求体校验错误 -> INVALID_PAYLOAD；
- 其余未预期异常 -> INTERNAL_ERROR，不透出底层堆栈。
"""
from __future__ import annotations

import logging

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from app.models import RejectReason

logger = logging.getLogger("observability.errors")


class DomainError(Exception):
    """领域错误基类，携带归一化的错误类型与稳定消息。"""

    def __init__(self, reason: RejectReason, message: str, status_code: int = 409):
        super().__init__(message)
        self.reason = reason
        self.message = message
        self.status_code = status_code


class DuplicateEventError(DomainError):
    def __init__(self, detail: str = ""):
        super().__init__(
            RejectReason.DUPLICATE_EVENT,
            detail or "重复事件：该事件已被接收",
            status_code=200,
        )


class InvalidPayloadError(DomainError):
    def __init__(self, detail: str = ""):
        super().__init__(
            RejectReason.INVALID_PAYLOAD,
            detail or "上报数据不合法",
            status_code=422,
        )


class ClockSkewError(DomainError):
    def __init__(self, detail: str = ""):
        super().__init__(
            RejectReason.CLOCK_SKEW,
            detail or "检测到时钟异常（时钟回拨）",
            status_code=422,
        )


class RunNotFoundError(DomainError):
    def __init__(self, detail: str = ""):
        super().__init__(
            RejectReason.RUN_NOT_FOUND,
            detail or "运行不存在",
            status_code=404,
        )


class TaskNotFoundError(DomainError):
    def __init__(self, detail: str = ""):
        super().__init__(
            RejectReason.TASK_NOT_FOUND,
            detail or "任务未注册",
            status_code=404,
        )


def _error_body(reason: RejectReason, message: str) -> dict:
    return {"error": reason.value, "message": message}


async def domain_error_handler(request: Request, exc: DomainError) -> JSONResponse:
    return JSONResponse(
        status_code=exc.status_code,
        content=_error_body(exc.reason, exc.message),
    )


async def validation_exception_handler(
    request: Request, exc: RequestValidationError
) -> JSONResponse:
    # 只保留字段级稳定信息，不透出 pydantic 内部结构
    fields: list[str] = []
    for err in exc.errors():
        loc = ".".join(str(p) for p in err.get("loc", []) if p != "body")
        fields.append(loc or "payload")
    message = "上报数据不合法: " + ", ".join(sorted(set(fields)))
    return JSONResponse(
        status_code=422,
        content=_error_body(RejectReason.INVALID_PAYLOAD, message),
    )


async def unhandled_error_handler(request: Request, exc: Exception) -> JSONResponse:
    # 底层异常只记录到服务端日志，对外归一为 INTERNAL_ERROR
    logger.exception("unhandled error: %s", exc)
    return JSONResponse(
        status_code=500,
        content=_error_body(
            RejectReason.INTERNAL_ERROR, "服务内部错误，请联系管理员"
        ),
    )


def register_error_handlers(app: FastAPI) -> None:
    app.add_exception_handler(DomainError, domain_error_handler)
    app.add_exception_handler(RequestValidationError, validation_exception_handler)
    app.add_exception_handler(Exception, unhandled_error_handler)
