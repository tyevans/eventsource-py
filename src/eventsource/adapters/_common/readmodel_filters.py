"""Shared `Filter` dispatch for read model repository adapters.

Both Python predicate evaluation and SQL clause rendering read from a single
operator table so operator coverage and NULL semantics never drift across
in-memory, SQLite, and PostgreSQL read model repositories.
"""

from __future__ import annotations

import operator
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any
from uuid import UUID

if TYPE_CHECKING:
    from eventsource.ports.readmodels import Filter, ReadModel


@dataclass(frozen=True)
class _Operator:
    """One row of the operator table -- both renderings of a single operator."""

    compare: Callable[[Any, Any], bool]
    matches_null: bool
    sql: str
    list_valued: bool = False
    empty_sql: str = ""


def _contains(value: Any, choices: Any) -> bool:
    return bool(value in choices)


def _not_contains(value: Any, choices: Any) -> bool:
    return bool(value not in choices)


# The single source of truth. A NULL-tolerant operator (`matches_null=True`)
# needs its SQL clause to say so explicitly, because SQL would otherwise
# evaluate it to UNKNOWN and drop the row.
_OPERATORS: dict[str, _Operator] = {
    "eq": _Operator(operator.eq, matches_null=False, sql="{field} = {value}"),
    "ne": _Operator(operator.ne, matches_null=True, sql="({field} IS NULL OR {field} != {value})"),
    "gt": _Operator(operator.gt, matches_null=False, sql="{field} > {value}"),
    "gte": _Operator(operator.ge, matches_null=False, sql="{field} >= {value}"),
    "lt": _Operator(operator.lt, matches_null=False, sql="{field} < {value}"),
    "lte": _Operator(operator.le, matches_null=False, sql="{field} <= {value}"),
    "in": _Operator(
        _contains,
        matches_null=False,
        sql="{field} IN {value}",
        list_valued=True,
        empty_sql="1 = 0",
    ),
    "not_in": _Operator(
        _not_contains,
        matches_null=True,
        sql="({field} IS NULL OR {field} NOT IN {value})",
        list_valued=True,
        empty_sql="1 = 1",
    ),
}


@dataclass(frozen=True)
class SqlDialect:
    """Dialect-specific list expansion and parameter coercion settings."""

    expand_lists: bool
    coerce: Callable[[Any], Any] = lambda value: value


def _sqlite_coerce(value: Any) -> Any:
    return str(value) if isinstance(value, UUID) else value


POSTGRESQL = SqlDialect(expand_lists=False)
SQLITE = SqlDialect(expand_lists=True, coerce=_sqlite_coerce)


def _resolve(model_class: type[ReadModel], filter_: Filter) -> _Operator:
    """Resolve the operator for `filter_` after validating the field exists."""
    if filter_.field not in model_class.model_fields:
        known = ", ".join(sorted(model_class.model_fields))
        raise ValueError(
            f"unknown field {filter_.field!r} for {model_class.__name__}; known fields: {known}"
        )
    try:
        return _OPERATORS[filter_.operator]
    except KeyError:
        raise ValueError(f"unknown filter operator: {filter_.operator!r}") from None


def matches_filter(model: ReadModel, filter_: Filter) -> bool:
    """Return whether `model` satisfies `filter_` for in-memory evaluation."""
    op = _resolve(type(model), filter_)
    value = getattr(model, filter_.field)
    if value is None:
        return op.matches_null
    return op.compare(value, filter_.value)


def check_filters(model_class: type[ReadModel], filters: Iterable[Filter]) -> None:
    """Validate every filter's field and operator before evaluating data."""
    for filter_ in filters:
        _resolve(model_class, filter_)


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
    "filter_to_sql",
    "matches_filter",
]
