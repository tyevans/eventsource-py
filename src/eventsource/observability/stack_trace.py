"""Frames-only stack trace for the exception being handled (TASK-0470).

Settled in ``observability/`` so the application and adapter rings can both use it
(the application ring may not import adapters).

`traceback.format_exc()` prints each exception's message, and the messages of
its chained causes. Pydantic and SQLAlchemy errors echo input values and bound
parameters there, so a DLQ row would store event payload. This module keeps the
frames and the exception type names and drops every message.
"""

from __future__ import annotations

import sys
import traceback


def frames_only_trace() -> str:
    """Return the frames of the active exception and its chain, without messages.

    Each exception in the chain gets a header line with its type name, followed
    by its frames. The chain runs from the outermost exception to the root cause.
    Returns "" when no exception is being handled.
    """
    active = sys.exc_info()[1]
    if active is None:
        return ""
    parts: list[str] = []
    seen: set[int] = set()
    exc: BaseException | None = active
    while exc is not None and id(exc) not in seen:
        seen.add(id(exc))
        parts.append(_block(exc))
        exc = _chained(exc)
    return "".join(parts)


def _block(exc: BaseException) -> str:
    frames = traceback.extract_tb(exc.__traceback__).format()
    return f"{type(exc).__qualname__}\n" + "".join(frames)


def _chained(exc: BaseException) -> BaseException | None:
    if exc.__cause__ is not None:
        return exc.__cause__
    if not exc.__suppress_context__:
        return exc.__context__
    return None


__all__ = ["frames_only_trace"]
