"""
Dialect differences between PostgreSQL and SQLite.

Repositories in this package serve both backends from a single implementation.
The backends differ in four ways that reach the SQL and the bound parameters:

- UUID: PostgreSQL has a native type; SQLite stores 36-character TEXT.
- Timestamps: PostgreSQL has TIMESTAMPTZ; SQLite stores ISO-8601 TEXT and
  returns it without timezone information.
- JSON: PostgreSQL has JSONB; SQLite stores TEXT.
- Current time: NOW() versus CURRENT_TIMESTAMP.
"""

from datetime import UTC, datetime
from enum import Enum
from typing import Any
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncConnection

from eventsource.adapters.serialization import json_dumps, json_loads


class Dialect(Enum):
    """Supported SQL dialects."""

    POSTGRESQL = "postgresql"
    SQLITE = "sqlite"


def dialect_of(conn: AsyncConnection) -> Dialect:
    """
    Determine the dialect of a live connection.

    Args:
        conn: An active SQLAlchemy async connection.

    Returns:
        The matching Dialect.

    Raises:
        ValueError: If the dialect is not supported by this library.
    """
    name = conn.dialect.name
    try:
        return Dialect(name)
    except ValueError:
        raise ValueError(
            f"Unsupported SQL dialect {name!r}. Supported dialects: {[d.value for d in Dialect]}"
        ) from None


def uuid_param(value: UUID | None, dialect: Dialect) -> str | UUID | None:
    """Encode a UUID for binding as a query parameter."""
    if value is None:
        return None
    return str(value) if dialect is Dialect.SQLITE else value


def uuid_result(value: object) -> UUID | None:
    """Decode a UUID from a result row, accepting either representation."""
    if value is None:
        return None
    if isinstance(value, UUID):
        return value
    if isinstance(value, bytes | bytearray | memoryview):
        return UUID(bytes=bytes(value))
    return UUID(str(value))


def ts_param(value: datetime, dialect: Dialect) -> str | datetime:
    """
    Encode a datetime for binding as a query parameter.

    Note: a naive datetime passed in comes back UTC-attached from
    `ts_result` (see its docstring).
    """
    return value.isoformat() if dialect is Dialect.SQLITE else value


def ts_result(value: object) -> datetime | None:
    """
    Decode a timestamp from a result row.

    SQLite returns naive ISO-8601 strings. We attach UTC rather than returning
    a naive datetime, so that callers can always compare results safely.
    """
    if value is None:
        return None
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(str(value))
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)


def json_param(value: object, dialect: Dialect) -> str | None:
    """
    Encode a JSON-serializable value for binding as a query parameter.

    Returns `None` for `None` input rather than the string `"null"`, so that
    Python `None` always becomes SQL NULL and a literal JSON null is never
    written. This keeps SQL NULL and stored JSON null distinguishable on
    read -- `json_result` cannot tell them apart once both are ambiguous.

    Delegates to `eventsource.adapters.serialization.json_dumps` (rather than plain
    `json.dumps`) so payloads containing UUID and datetime values encode
    without raising `TypeError`.

    Ignores `dialect` deliberately: asyncpg's JSONB binding also accepts a
    JSON string, so one encoding serves both backends. The parameter is kept
    for call-site symmetry with the other adapters.
    """
    if value is None:
        return None
    return json_dumps(value)


def json_result(value: object) -> Any:
    """
    Decode a JSON value from a result row, accepting text or parsed JSON.

    Delegates to `eventsource.adapters.serialization.json_loads` (rather than plain
    `json.loads`) so decoding routes through the same encoder that
    `json_param` used to encode -- if `json_loads` is later backed by
    orjson, this call site moves with it instead of silently continuing to
    decode with stdlib.
    """
    if value is None:
        return None
    if isinstance(value, str | bytes):
        return json_loads(value)
    return value


def now_expr(dialect: Dialect) -> str:
    """SQL expression for the current timestamp in this dialect."""
    return "NOW()" if dialect is Dialect.POSTGRESQL else "CURRENT_TIMESTAMP"


__all__ = [
    "Dialect",
    "dialect_of",
    "json_param",
    "json_result",
    "now_expr",
    "ts_param",
    "ts_result",
    "uuid_param",
    "uuid_result",
]
