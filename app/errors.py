"""归一化错误类型：对外只暴露可区分的错误码，不透出底层异常。"""


class ObservabilityError(Exception):
    """所有可观测性领域错误的基类。"""

    code: str = "INTERNAL_ERROR"
    http_status: int = 500
    message: str = "internal error"

    def __init__(self, message: str | None = None, *, detail: dict | None = None):
        super().__init__(message or self.message)
        if message:
            self.message = message
        self.detail = detail or {}


class InvalidEventError(ObservabilityError):
    code = "INVALID_EVENT"
    http_status = 422
    message = "event payload failed validation"


class DuplicateEventError(ObservabilityError):
    code = "DUPLICATE_EVENT"
    http_status = 409
    message = "event already accepted for this run"


class StaleEventError(ObservabilityError):
    code = "EVENT_STALE"
    http_status = 409
    message = "event is older than the already applied high-water mark"


class RunAlreadyTerminalError(ObservabilityError):
    code = "RUN_ALREADY_TERMINAL"
    http_status = 409
    message = "run already reached a terminal state"


class UnknownRunError(ObservabilityError):
    code = "UNKNOWN_RUN"
    http_status = 404
    message = "run does not exist"


class UnknownTaskError(ObservabilityError):
    code = "UNKNOWN_TASK"
    http_status = 404
    message = "task is not registered"


class ClockSkewError(ObservabilityError):
    code = "CLOCK_SKEW"
    http_status = 422
    message = "event timestamp moved backwards beyond tolerance"
