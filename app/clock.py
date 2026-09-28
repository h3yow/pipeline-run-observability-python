"""时间来源抽象，测试时可注入固定时钟。"""

from __future__ import annotations

import time
from collections.abc import Callable


class Clock:
    def __init__(self, time_fn: Callable[[], float] = time.time):
        self._time_fn = time_fn

    def now(self) -> float:
        return self._time_fn()
