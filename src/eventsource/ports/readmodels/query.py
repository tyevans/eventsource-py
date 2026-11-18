"""
Query builder for read model repositories.

Provides a database-agnostic way to express queries with filters,
ordering, and pagination. Repository implementations translate
these queries to their specific backend syntax (SQL, MongoDB, etc.).
"""

from dataclasses import dataclass, field, replace
from typing import Any, Literal

from eventsource.ports.readmodels.query_filter import Filter
from eventsource.ports.readmodels.query_validation import validate_query_values


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

    def __post_init__(self) -> None:
        validate_query_values(self)

    @classmethod
    def where(cls, *filters: Filter, **eq_filters: Any) -> "Query":
        """Create a Query with filters and/or keyword equality conditions."""
        return cls().with_filter(*filters, **eq_filters)

    def with_filter(self, *filters: Filter, **eq_filters: Any) -> "Query":
        """Create a new Query with additional filters and/or keyword equality conditions."""
        eq_list = [Filter.eq(k, v) for k, v in eq_filters.items()]
        return replace(self, filters=[*self.filters, *filters, *eq_list])

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


__all__ = ["Filter", "Query", "validate_query_values"]
