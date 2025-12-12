"""
Query builder for read model repositories.

Provides a database-agnostic way to express queries with filters,
ordering, and pagination. Repository implementations translate
these queries to their specific backend syntax (SQL, MongoDB, etc.).
"""

from dataclasses import dataclass, field
from typing import Any, Literal


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

    Supported Operators:
        - eq: Equal (field = value)
        - ne: Not equal (field != value)
        - gt: Greater than (field > value)
        - gte: Greater than or equal (field >= value)
        - lt: Less than (field < value)
        - lte: Less than or equal (field <= value)
        - in: Value in list (field IN (value1, value2, ...))
        - not_in: Value not in list (field NOT IN (value1, value2, ...))

    Example:
        >>> # Using factory methods (recommended)
        >>> f1 = Filter.eq("status", "active")
        >>> f2 = Filter.gt("amount", 100)
        >>> f3 = Filter.in_("status", ["pending", "processing"])
        >>>
        >>> # Direct construction
        >>> f4 = Filter(field="status", operator="eq", value="active")
    """

    field: str
    operator: Literal["eq", "ne", "gt", "gte", "lt", "lte", "in", "not_in"]
    value: Any

    @classmethod
    def eq(cls, field: str, value: Any) -> "Filter":
        """
        Create an equality filter (field = value).

        Args:
            field: Field name to filter on
            value: Value to match

        Returns:
            Filter with eq operator

        Example:
            >>> Filter.eq("status", "active")
            Filter(field='status', operator='eq', value='active')
        """
        return cls(field=field, operator="eq", value=value)

    @classmethod
    def ne(cls, field: str, value: Any) -> "Filter":
        """
        Create a not-equal filter (field != value).

        Args:
            field: Field name to filter on
            value: Value that should not match

        Returns:
            Filter with ne operator

        Example:
            >>> Filter.ne("status", "cancelled")
            Filter(field='status', operator='ne', value='cancelled')
        """
        return cls(field=field, operator="ne", value=value)

    @classmethod
    def gt(cls, field: str, value: Any) -> "Filter":
        """
        Create a greater-than filter (field > value).

        Args:
            field: Field name to filter on
            value: Value to compare against

        Returns:
            Filter with gt operator

        Example:
            >>> Filter.gt("amount", Decimal("100.00"))
            Filter(field='amount', operator='gt', value=Decimal('100.00'))
        """
        return cls(field=field, operator="gt", value=value)

    @classmethod
    def gte(cls, field: str, value: Any) -> "Filter":
        """
        Create a greater-than-or-equal filter (field >= value).

        Args:
            field: Field name to filter on
            value: Value to compare against

        Returns:
            Filter with gte operator

        Example:
            >>> Filter.gte("version", 2)
            Filter(field='version', operator='gte', value=2)
        """
        return cls(field=field, operator="gte", value=value)

    @classmethod
    def lt(cls, field: str, value: Any) -> "Filter":
        """
        Create a less-than filter (field < value).

        Args:
            field: Field name to filter on
            value: Value to compare against

        Returns:
            Filter with lt operator

        Example:
            >>> Filter.lt("created_at", datetime.now(UTC))
            Filter(field='created_at', operator='lt', value=datetime(...))
        """
        return cls(field=field, operator="lt", value=value)

    @classmethod
    def lte(cls, field: str, value: Any) -> "Filter":
        """
        Create a less-than-or-equal filter (field <= value).

        Args:
            field: Field name to filter on
            value: Value to compare against

        Returns:
            Filter with lte operator

        Example:
            >>> Filter.lte("priority", 5)
            Filter(field='priority', operator='lte', value=5)
        """
        return cls(field=field, operator="lte", value=value)

    @classmethod
    def in_(cls, field: str, values: list[Any]) -> "Filter":
        """
        Create an "in list" filter (field IN (values)).

        Args:
            field: Field name to filter on
            values: List of values to match

        Returns:
            Filter with in operator

        Example:
            >>> Filter.in_("status", ["pending", "processing", "shipped"])
            Filter(field='status', operator='in', value=['pending', 'processing', 'shipped'])
        """
        return cls(field=field, operator="in", value=values)

    @classmethod
    def not_in(cls, field: str, values: list[Any]) -> "Filter":
        """
        Create a "not in list" filter (field NOT IN (values)).

        Args:
            field: Field name to filter on
            values: List of values that should not match

        Returns:
            Filter with not_in operator

        Example:
            >>> Filter.not_in("status", ["cancelled", "refunded"])
            Filter(field='status', operator='not_in', value=['cancelled', 'refunded'])
        """
        return cls(field=field, operator="not_in", value=values)

    def __str__(self) -> str:
        """Human-readable string representation."""
        op_symbols = {
            "eq": "=",
            "ne": "!=",
            "gt": ">",
            "gte": ">=",
            "lt": "<",
            "lte": "<=",
            "in": "IN",
            "not_in": "NOT IN",
        }
        return f"{self.field} {op_symbols[self.operator]} {self.value!r}"


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

    Example:
        >>> from eventsource.readmodels import Query, Filter
        >>>
        >>> # Find shipped orders over $100, newest first
        >>> query = Query(
        ...     filters=[
        ...         Filter.eq("status", "shipped"),
        ...         Filter.gt("total_amount", Decimal("100")),
        ...     ],
        ...     order_by="created_at",
        ...     order_direction="desc",
        ...     limit=20,
        ...     offset=0,
        ... )
        >>>
        >>> results = await repo.find(query)

    Note:
        - Filters are combined with AND logic only (no OR support in this version)
        - `include_deleted=True` shows soft-deleted records
        - Without `limit`, queries may return large result sets
    """

    filters: list[Filter] = field(default_factory=list)
    order_by: str | None = None
    order_direction: Literal["asc", "desc"] = "asc"
    limit: int | None = None
    offset: int = 0
    include_deleted: bool = False

    def with_filter(self, filter_: Filter) -> "Query":
        """
        Create a new Query with an additional filter.

        Args:
            filter_: Filter to add

        Returns:
            New Query with the filter added

        Example:
            >>> base_query = Query(limit=100)
            >>> filtered = base_query.with_filter(Filter.eq("status", "active"))
        """
        return Query(
            filters=[*self.filters, filter_],
            order_by=self.order_by,
            order_direction=self.order_direction,
            limit=self.limit,
            offset=self.offset,
            include_deleted=self.include_deleted,
        )

    def with_order(self, field: str, direction: Literal["asc", "desc"] = "asc") -> "Query":
        """
        Create a new Query with ordering.

        Args:
            field: Field name to order by
            direction: Sort direction ('asc' or 'desc')

        Returns:
            New Query with ordering set

        Example:
            >>> query = Query().with_order("created_at", "desc")
        """
        return Query(
            filters=self.filters.copy(),
            order_by=field,
            order_direction=direction,
            limit=self.limit,
            offset=self.offset,
            include_deleted=self.include_deleted,
        )

    def with_pagination(self, limit: int, offset: int = 0) -> "Query":
        """
        Create a new Query with pagination.

        Args:
            limit: Maximum number of records to return
            offset: Number of records to skip

        Returns:
            New Query with pagination set

        Example:
            >>> # Page 2 with 20 items per page
            >>> query = Query().with_pagination(limit=20, offset=20)
        """
        return Query(
            filters=self.filters.copy(),
            order_by=self.order_by,
            order_direction=self.order_direction,
            limit=limit,
            offset=offset,
            include_deleted=self.include_deleted,
        )

    def with_deleted(self, include: bool = True) -> "Query":
        """
        Create a new Query that includes or excludes soft-deleted records.

        Args:
            include: Whether to include soft-deleted records

        Returns:
            New Query with include_deleted set

        Example:
            >>> # Include deleted records for admin view
            >>> query = Query().with_deleted(True)
        """
        return Query(
            filters=self.filters.copy(),
            order_by=self.order_by,
            order_direction=self.order_direction,
            limit=self.limit,
            offset=self.offset,
            include_deleted=include,
        )

    def __str__(self) -> str:
        """Human-readable string representation."""
        parts = []
        if self.filters:
            filter_strs = " AND ".join(str(f) for f in self.filters)
            parts.append(f"WHERE {filter_strs}")
        if self.order_by:
            parts.append(f"ORDER BY {self.order_by} {self.order_direction.upper()}")
        if self.limit is not None:
            parts.append(f"LIMIT {self.limit}")
        if self.offset:
            parts.append(f"OFFSET {self.offset}")
        if self.include_deleted:
            parts.append("(including deleted)")
        return " ".join(parts) if parts else "(all records)"
