"""任务期望周期等元数据注册表。"""

from __future__ import annotations

import threading

from .models import TaskSpec


class TaskRegistry:
    def __init__(self) -> None:
        self._specs: dict[str, TaskSpec] = {}
        self._lock = threading.RLock()

    def register(self, spec: TaskSpec) -> TaskSpec:
        """注册或覆盖任务期望周期配置，返回最终生效的配置。"""
        with self._lock:
            self._specs[spec.task_id] = spec
            return spec

    def get(self, task_id: str) -> TaskSpec | None:
        with self._lock:
            return self._specs.get(task_id)

    def list_all(self) -> list[TaskSpec]:
        with self._lock:
            return list(self._specs.values())
