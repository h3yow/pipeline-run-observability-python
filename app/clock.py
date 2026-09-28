"""单调时钟源，隔离对系统时钟的依赖以便测试。"""
from __future__ import annotations

import time


class Clock:
    """时钟抽象：所有“现在”的取值都从这里来。"""

    def now(self) -> float:
        raise NotImplementedError


class SystemClock(Clock):
    """系统单调时钟，不受墙上时间回拨影响。"""

    def now(self) -> float:
        return time.monotonic()


class ManualClock(Clock):
    """测试用可控时钟；只允许前进，显式回退用于模拟时钟回拨。"""

    def __init__(self, start: float = 1000.0):
        self._now = float(start)

    def now(self) -> float:
        return self._now

    def advance(self, seconds: float) -> float:
        if seconds < 0:
            raise ValueError("advance 不接受负值；如需模拟回拨请使用 set")
        self._now += seconds
        return self._now

    def set(self, value: float) -> float:
        self._now = float(value)
        return self._now
