"""Shared `Filter` dispatch for read model repository adapters.

Both Python predicate evaluation and SQL clause rendering read from a single
operator table so operator coverage and NULL semantics never drift across
in-memory, SQLite, and PostgreSQL read model repositories.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any
from uuid import UUID

from eventsource.adapters._common.readmodel_operators import _resolve, check_filters, matches_filter
from eventsource.ports.readmodels.query import Query, validate_query_values

if TYPE_CHECKING:
    from eventsource.ports.readmodels import Filter, ReadModel


@dataclass(frozen=True)
class SqlDialect:
    """Dialect-specific list expansion and parameter coercion settings."""

    expand_lists: bool
    coerce: Callable[[Any], Any] = lambda value: value


def _sqlite_coerce(value: Any) -> Any:
    return str(value) if isinstance(value, UUID) else value


POSTGRESQL = SqlDialect(expand_lists=False)
SQLITE = SqlDialect(expand_lists=True, coerce=_sqlite_coerce)


def check_query(model_class: type[ReadModel], query: Query) -> None:
    """Validate a whole query before any adapter evaluates or renders it.

    Every read model adapter calls this first, so the same input is accepted
    or rejected the same way on each backend. `order_by` must name a model
    field, which makes it safe to render as an identifier. Paging and
    direction values are re-checked here because `Query` is a mutable
    dataclass and its fields can change after construction.
    """
    validate_query_values(query)
    check_filters(model_class, query.filters)
    if query.order_by is not None and query.order_by not in model_class.model_fields:
        known = ", ".join(sorted(model_class.model_fields))
        raise ValueError(
            f"unknown field {query.order_by!r} for {model_class.__name__}; known fields: {known}"
        )


def order_by_sql(query: Query) -> str:
    """Render the ORDER BY clause for a query that passed `check_query`."""
    if not query.order_by:
        return ""
    return f"ORDER BY {query.order_by} {query.order_direction.upper()}"


def filter_to_sql(
    model_class: type[ReadModel],
    filter_: Filter,
    dialect: SqlDialect,
    bind: Callable[[Any], str],
) -> str:
    """Render `filter_` as a SQL clause, binding its values through `bind`."""
    op = _resolve(model_class, filter_)
    field = filter_.field

    if not op.list_valued:
        return op.sql.format(field=field, value=bind(dialect.coerce(filter_.value)))

    values = [dialect.coerce(v) for v in filter_.value]
    if not values:
        return op.empty_sql
    if dialect.expand_lists:
        rendered = "(" + ",".join(bind(v) for v in values) + ")"
        return op.sql.format(field=field, value=rendered)
    # A single array parameter: `IN (...)` has no array form, so the
    # membership tests become `= ANY` / `!= ALL` over the bound array.
    placeholder = bind(values)
    array_sql = {"in": f"{field} = ANY({placeholder})"}.get(
        filter_.operator,
        f"({field} IS NULL OR {field} != ALL({placeholder}))",
    )
    return array_sql


__all__ = [
    "POSTGRESQL",
    "SQLITE",
    "SqlDialect",
    "check_filters",
    "check_query",
    "filter_to_sql",
    "matches_filter",
    "order_by_sql",
]
