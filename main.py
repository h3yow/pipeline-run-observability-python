"""开发入口：生产/测试请使用 app.main:app 或 create_app()。"""
from __future__ import annotations

import uvicorn

from app.main import app

__all__ = ["app"]


if __name__ == "__main__":
    uvicorn.run("app.main:app", host="127.0.0.1", port=8000)
