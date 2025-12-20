from __future__ import annotations

import logging
import sys
from typing import Any

import structlog
from structlog.contextvars import bind_contextvars, clear_contextvars, merge_contextvars

_CONFIGURED = False

from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class ILogger(Protocol):
    def debug(self, event: str, **kw: Any) -> Any: ...
    def info(self, event: str, **kw: Any) -> Any: ...
    def warning(self, event: str, **kw: Any) -> Any: ...
    def error(self, event: str, **kw: Any) -> Any: ...
    def exception(self, event: str, **kw: Any) -> Any: ...
    def bind(self, **kw: Any) -> "ILogger": ...


def configure_logging(*, level: str = "INFO", fmt: str = "console") -> None:
    global _CONFIGURED
    if _CONFIGURED:
        return

    root = logging.getLogger()
    root.handlers.clear()
    root.setLevel(level.upper())

    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setLevel(level.upper())
    handler.setFormatter(logging.Formatter("%(message)s"))
    root.addHandler(handler)

    processors: list[Any] = [
        merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
    ]

    if fmt == "console":
        processors.append(structlog.dev.ConsoleRenderer())
    else:
        processors.append(structlog.processors.JSONRenderer())

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.make_filtering_bound_logger(level.upper()),
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )

    _CONFIGURED = True


def get_logger(name: str = "hk_public_transport") -> structlog.stdlib.BoundLogger:
    return structlog.get_logger(name)


def bind(**values: Any) -> None:
    bind_contextvars(**values)


def clear_bindings() -> None:
    clear_contextvars()
