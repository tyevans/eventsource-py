"""Summaries of exceptions that are safe to log, store, and report.

``str(exc)`` can carry event payload values. Pydantic echoes ``input_value``
in its messages, and SQLAlchemy appends bound ``[parameters: ...]`` to
statement errors. This module replaces those two message texts with structure
that holds no values. Other exceptions keep their message, which is capped in
length.

- ``ValidationError``: the class name and each error's ``(loc, type)``.
- ``StatementError``: the class name and the class of its DBAPI ``orig``.
- Anything else: the class name and ``str(exc)``, capped at 500 characters.
"""

from __future__ import annotations

import re

from pydantic import ValidationError

_MAX_SUMMARY_LENGTH = 1000
_MAX_MESSAGE_LENGTH = 500
_CONTROL_CHARS = re.compile(r"[\x00-\x1f\x7f]")


def _statement_error_detail(error: BaseException) -> str:
    """Return the DBAPI error class of a SQLAlchemy ``StatementError``, or ``""``.

    The check uses class names, so this module does not import SQLAlchemy.
    """
    if not any(cls.__name__ == "StatementError" for cls in type(error).__mro__):
        return ""
    orig = getattr(error, "orig", None)
    return f"orig={type(orig).__name__}" if orig is not None else ""


def _validation_detail(error: ValidationError) -> str:
    """Return ``(loc, type)`` pairs for each validation error, never input values."""
    parts = [
        f"{tuple(item.get('loc', ()))!r}:{item.get('type', 'unknown')}" for item in error.errors()
    ]
    return f"{len(parts)} error(s): " + "; ".join(parts)


def _escape_control(match: re.Match[str]) -> str:
    return f"\\x{ord(match.group()):02x}"


def error_summary(error: BaseException) -> str:
    """Return a log-safe summary of an exception.

    A pydantic error becomes ``ValidationError(1 error(s): ('amount',):int_parsing)``
    and never contains the input values. A SQLAlchemy statement error never
    contains the SQL text or bound parameters. Other exceptions keep their
    message, capped at 500 characters.

    Args:
        error: The exception to summarize.

    Returns:
        The class name with structural detail or message, capped at 1000 characters.
    """
    name = type(error).__name__
    if isinstance(error, ValidationError):
        text = f"{name}({_validation_detail(error)})"
    elif detail := _statement_error_detail(error):
        text = f"{name}({detail})"
    else:
        # Control characters are escaped so a message cannot forge a log line (CWE-117).
        message = _CONTROL_CHARS.sub(_escape_control, str(error)[:_MAX_MESSAGE_LENGTH])
        text = f"{name}: {message}" if message else name
    return text[:_MAX_SUMMARY_LENGTH]


__all__ = ["error_summary"]
