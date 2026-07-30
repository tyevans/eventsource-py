"""
TRANSITION: Dialect helpers moved to adapters._sql.dialect.

This module re-exports from the new location for backward compatibility.
Imports should migrate to eventsource.adapters._sql.dialect over time.
"""

from eventsource.adapters._sql.dialect import (
    Dialect,
    dialect_of,
    json_param,
    json_result,
    now_expr,
    ts_param,
    ts_result,
    uuid_param,
    uuid_result,
)

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
