"""
Query builder for read model repositories.

Provides a database-agnostic way to express queries with filters,
ordering, and pagination. Repository implementations translate
these queries to their specific backend syntax (SQL, MongoDB, etc.).
"""

from dataclasses import dataclass, field, replace
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
    def eq(cls, field: str, value: Any) -> "Filter":
        """Create an equality filter (field = value)."""
        return cls(field=field, operator="eq", value=value)

    @classmethod
    def ne(cls, field: str, value: Any) -> "Filter":
        """Create a not-equal filter (field != value)."""
        return cls(field=field, operator="ne", value=value)

    @classmethod
    def gt(cls, field: str, value: Any) -> "Filter":
        """Create a greater-than filter (field > value)."""
        return cls(field=field, operator="gt", value=value)

    @classmethod
    def gte(cls, field: str, value: Any) -> "Filter":
        """Create a greater-than-or-equal filter (field >= value)."""
        return cls(field=field, operator="gte", value=value)

    @classmethod
    def lt(cls, field: str, value: Any) -> "Filter":
        """Create a less-than filter (field < value)."""
        return cls(field=field, operator="lt", value=value)

    @classmethod
    def lte(cls, field: str, value: Any) -> "Filter":
        """Create a less-than-or-equal filter (field <= value)."""
        return cls(field=field, operator="lte", value=value)

    @classmethod
    def in_(cls, field: str, values: list[Any]) -> "Filter":
        """Create an "in list" filter (field IN (values))."""
        return cls(field=field, operator="in", value=values)

    @classmethod
    def not_in(cls, field: str, values: list[Any]) -> "Filter":
        """Create a "not in list" filter (field NOT IN (values))."""
        return cls(field=field, operator="not_in", value=values)

    def __str__(self) -> str:
        """Human-readable string representation."""
        return f"{self.field} {_OP_SYMBOLS[self.operator]} {self.value!r}"


@dataclass
class Query:
    """
    Query specification for read model repositories.

    Combines filters, ordering, and pagination into a single query object.
    All filters are combined with AND logic.

    Attributes:
        filters: List of Filter conditions (combined with AND)
        order_by: Field name to order results by
        order_direction: Sort direction ('asc' or 'desc')
        limit: Maximum number of records to return
        offset: Number of records to skip (for pagination)
        include_deleted: Whether to include soft-deleted records
    """

    filters: list[Filter] = field(default_factory=list)
    order_by: str | None = None
    order_direction: Literal["asc", "desc"] = "asc"
    limit: int | None = None
    offset: int = 0
    include_deleted: bool = False

    def with_filter(self, filter_: Filter) -> "Query":
        """Create a new Query with an additional filter."""
        return replace(self, filters=[*self.filters, filter_])

    def with_order(self, field: str, direction: Literal["asc", "desc"] = "asc") -> "Query":
        """Create a new Query with ordering."""
        return replace(self, filters=self.filters.copy(), order_by=field, order_direction=direction)

    def with_pagination(self, limit: int, offset: int = 0) -> "Query":
        """Create a new Query with pagination."""
        return replace(self, filters=self.filters.copy(), limit=limit, offset=offset)

    def with_deleted(self, include: bool = True) -> "Query":
        """Create a new Query that includes or excludes soft-deleted records."""
        return replace(self, filters=self.filters.copy(), include_deleted=include)

    def __str__(self) -> str:
        """Human-readable string representation."""
        parts = []
        if self.filters:
            parts.append(f"WHERE {' AND '.join(str(f) for f in self.filters)}")
        if self.order_by:
            parts.append(f"ORDER BY {self.order_by} {self.order_direction.upper()}")
        if self.limit is not None:
            parts.append(f"LIMIT {self.limit}")
        if self.offset:
            parts.append(f"OFFSET {self.offset}")
        if self.include_deleted:
            parts.append("(including deleted)")
        return " ".join(parts) if parts else "(all records)"


__all__ = ["Filter", "Query"]
