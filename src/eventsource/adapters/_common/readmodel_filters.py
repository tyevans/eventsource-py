"""Shared `Filter` dispatch for read model repository adapters.

Every `ReadModelRepository` has to answer the same question: does this row
satisfy this `Filter`? The answer is port semantics -- it depends on
`Filter.operator` and on the read model's fields, and on nothing a backend
knows -- so a backend that re-derived it could only ever diverge from its
siblings.

Which is what happened. Three adapters carried the same eight-branch
`if/elif` chain, and all three disagreed:

- the in-memory adapter's chain ended in `else: return False`, so an
  unrecognized operator silently dropped every row instead of failing;
- `ne` / `not_in` against a NULL column matched in memory (Python's
  `None != "x"` is true) and did not match in SQL (three-valued logic);
- an unknown field name was a silent no-match in memory and a driver error
  in SQL;
- SQLite emitted `OFFSET` without `LIMIT`, which is a syntax error there.

**A behavior the conformance suite asserts is implemented once.** The
operator table below is that one place: both renderers -- the Python
predicate and the SQL clause -- read from it, so an operator cannot exist
for one backend and not another, and its null handling cannot drift.

The chosen semantics are documented on `ReadModelRepository.find`; the
matrix that pins them lives in
`eventsource.testing.conformance_ports.readmodels`.
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
    """One row of the operator table -- both renderings of a single operator.

    Attributes:
        compare: Applied to `(model_value, filter_value)` when the model
            value is not None.
        matches_null: The result when the model value is None. Only `ne` and
            `not_in` say True: a field with no value differs from every
            value, but is neither equal to nor ordered against one.
        sql: Clause template over `{field}` and `{value}`, where `{value}` is
            an already-bound placeholder (or a placeholder list).
        list_valued: Whether `Filter.value` is a collection.
        empty_sql: Clause for a list-valued operator given an empty list.
    """

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
    """What a SQL backend does differently while rendering the same filter.

    Attributes:
        expand_lists: True when `IN` takes one placeholder per element
            (SQLite); False when the whole list binds as a single array
            parameter (PostgreSQL, via `= ANY` / `!= ALL`).
        coerce: Applied to every bound value. SQLite stores UUIDs as text
            and must compare against text.
    """

    expand_lists: bool
    coerce: Callable[[Any], Any] = lambda value: value


def _sqlite_coerce(value: Any) -> Any:
    return str(value) if isinstance(value, UUID) else value


POSTGRESQL = SqlDialect(expand_lists=False)
SQLITE = SqlDialect(expand_lists=True, coerce=_sqlite_coerce)


def _resolve(model_class: type[ReadModel], filter_: Filter) -> _Operator:
    """The operator for `filter_`, after checking the field exists.

    Raises:
        ValueError: If the field is not a field of `model_class`, or the
            operator is not one of the eight in `Filter`. Both are caller
            bugs; neither may be reported as "nothing matched".
    """
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
    """Whether `model` satisfies `filter_`, for repositories that filter in Python.

    Args:
        model: The read model to test.
        filter_: The condition to apply.

    Returns:
        True if the model matches.

    Raises:
        ValueError: On an unknown field name or an unknown operator.
    """
    op = _resolve(type(model), filter_)
    value = getattr(model, filter_.field)
    if value is None:
        return op.matches_null
    return op.compare(value, filter_.value)


def check_filters(model_class: type[ReadModel], filters: Iterable[Filter]) -> None:
    """Validate every filter's field and operator, before looking at any data.

    A repository that filters in Python never evaluates its predicate when
    the candidate set is empty, so a typo'd field would surface only on
    populated stores. The SQL backends render their clauses whether or not
    a row exists; this makes the in-memory backend fail the same way.

    Raises:
        ValueError: On an unknown field name or an unknown operator.
    """
    for filter_ in filters:
        _resolve(model_class, filter_)


def filter_to_sql(
    model_class: type[ReadModel],
    filter_: Filter,
    dialect: SqlDialect,
    bind: Callable[[Any], str],
) -> str:
    """Render `filter_` as a SQL clause, binding its values through `bind`.

    Args:
        model_class: The read model, for field-name validation.
        filter_: The condition to render.
        dialect: How this backend spells list membership and stores UUIDs.
        bind: Registers one parameter value and returns the placeholder text
            for it (`?` for SQLite, `:p3` for PostgreSQL). Called once per
            bound value, left to right, so positional backends stay in order.

    Returns:
        A SQL boolean expression.

    Raises:
        ValueError: On an unknown field name or an unknown operator.
    """
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
