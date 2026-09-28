import logging

import uvicorn
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from app.api import router
from app.errors import ObservabilityError
from app.service import ObservabilityService

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("pipeline-observability")

app = FastAPI(title="Pipeline Run Observability")
app.state.service = ObservabilityService()
app.include_router(router)


def _error_json(status_code: int, code: str, message: str, detail: dict | None = None):
    return JSONResponse(
        status_code=status_code,
        content={"error_code": code, "message": message, "detail": detail or {}},
    )


@app.exception_handler(ObservabilityError)
async def observability_error_handler(request: Request, exc: ObservabilityError) -> JSONResponse:
    logger.info("rejected %s %s code=%s detail=%s",
                request.method, request.url.path, exc.code, exc.detail)
    return _error_json(exc.http_status, exc.code, exc.message, exc.detail)


@app.exception_handler(RequestValidationError)
async def validation_error_handler(request: Request, exc: RequestValidationError) -> JSONResponse:
    # 协议层/字段层校验失败统一归一为 INVALID_EVENT，不把 pydantic 结构直接透出
    return _error_json(
        422,
        "INVALID_EVENT",
        "event payload failed validation",
        {"fields": _simplify_errors(exc.errors())},
    )


@app.exception_handler(Exception)
async def unhandled_error_handler(request: Request, exc: Exception) -> JSONResponse:
    # 兜底：记录原始异常，对外只返回归一错误，避免底层堆栈/信息泄露
    logger.exception("unhandled error on %s %s", request.method, request.url.path)
    return _error_json(500, "INTERNAL_ERROR", "internal error")


def _simplify_errors(errors: list[dict]) -> list[dict]:
    simplified = []
    for err in errors:
        simplified.append({
            "location": [str(loc) for loc in err.get("loc", [])],
            "message": err.get("msg", ""),
            "type": err.get("type", ""),
        })
    return simplified


@app.get("/")
def read_root():
    return {"service": "pipeline-run-observability", "status": "ok"}


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=0)
