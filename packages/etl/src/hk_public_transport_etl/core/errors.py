from __future__ import annotations

import traceback
from dataclasses import dataclass


class ETLError(RuntimeError):
    """Base error"""


@dataclass(frozen=True, slots=True)
class StageError:
    """
    A normalized error record for stage failures.
    """

    exc_type: str
    message: str
    traceback: str


def stage_error_from_exc(exc: BaseException) -> StageError:
    return StageError(
        exc_type=type(exc).__name__,
        message=str(exc),
        traceback=traceback.format_exc(),
    )


class TransientError(ETLError):
    """
    Retryable failures such as network timeouts, temporary upstream 5xx
    """


class ExternalServiceError(TransientError):
    """Upstream service failure"""


class InputDataError(ETLError):
    """
    Non-retryable: upstream content is present but invalid w.r.t. expectations
    (schema mismatch, parse failure due to format changes, impossible invariants)
    """


class InternalError(ETLError):
    """Bugs or invariant violation in our code"""


class ParseError(ETLError):
    """Parse-stage error"""


class NormalizeError(ETLError):
    """Normalize-stage error"""


class CommitError(ETLError):
    """Commit-stage error"""


class PublishError(ETLError):
    """Publish-stage error"""
