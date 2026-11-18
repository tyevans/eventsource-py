"""`Filter`: one field condition for a read model `Query` (re-exported by `query.py`)."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any, Literal

_OP_SYMBOLS: dict[str, str] = {
    "eq": "=",
    "ne": "!=",
    "gt": ">",
    "gte": ">=",
    "lt": "<",
    "lte": "<=",
    "in": "IN",
    "not_in": "NOT IN",
}


@dataclass(frozen=True)
class Filter:
    """
    A single filter condition for a query.

    Filters are immutable and represent a condition like "status = 'active'"
    or "amount > 100". Use the factory class methods for type-safe construction.

    Attributes:
        field: Name of the field to filter on
        operator: Comparison operator (eq, ne, gt, gte, lt, lte, in, not_in)
        value: Value to compare against
    """

    field: str
    operator: Literal["eq", "ne", "gt", "gte", "lt", "lte", "in", "not_in"]
    value: Any

    @classmethod
    def eq(cls, field: str, value: Any) -> Filter:
        """Create an equality filter (field = value)."""
        return cls(field=field, operator="eq", value=value)

    @classmethod
    def ne(cls, field: str, value: Any) -> Filter:
        """Create a not-equal filter (field != value)."""
        return cls(field=field, operator="ne", value=value)

    @classmethod
    def gt(cls, field: str, value: Any) -> Filter:
        """Create a greater-than filter (field > value)."""
        return cls(field=field, operator="gt", value=value)

    @classmethod
    def gte(cls, field: str, value: Any) -> Filter:
        """Create a greater-than-or-equal filter (field >= value)."""
        return cls(field=field, operator="gte", value=value)

    @classmethod
    def lt(cls, field: str, value: Any) -> Filter:
        """Create a less-than filter (field < value)."""
        return cls(field=field, operator="lt", value=value)

    @classmethod
    def lte(cls, field: str, value: Any) -> Filter:
        """Create a less-than-or-equal filter (field <= value)."""
        return cls(field=field, operator="lte", value=value)

    @classmethod
    def in_(cls, field: str, values: Iterable[Any]) -> Filter:
        """Create an "in list" filter (field IN (values))."""
        return cls(field=field, operator="in", value=list(values))

    @classmethod
    def not_in(cls, field: str, values: Iterable[Any]) -> Filter:
        """Create a "not in list" filter (field NOT IN (values))."""
        return cls(field=field, operator="not_in", value=list(values))

    def __str__(self) -> str:
        """Human-readable string representation."""
        return f"{self.field} {_OP_SYMBOLS[self.operator]} {self.value!r}"


__all__ = ["Filter"]
