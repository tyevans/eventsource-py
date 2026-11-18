"""Validation for `Query` values: sort direction, paging, and field-name shape."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from eventsource.ports.readmodels.query import Query

_DIRECTIONS: frozenset[str] = frozenset({"asc", "desc"})


def validate_query_values(query: Query) -> None:
    """
    Check the model-free fields of `query` and normalise the sort direction.

    `order_direction` accepts `asc` or `desc` in any case and is stored in
    lower case. `limit` (None or a non-negative int) and `offset` (a
    non-negative int) must be plain integers. `order_by` must be a string or
    None; whether it names a model field is checked by the adapters.

    Raises:
        ValueError: If a value has the wrong type or is out of range.
    """
    if query.order_by is not None and not isinstance(query.order_by, str):
        raise ValueError(f"order_by must be a field name or None, got {query.order_by!r}")
    direction = query.order_direction
    if not isinstance(direction, str) or direction.lower() not in _DIRECTIONS:
        raise ValueError(f"order_direction must be 'asc' or 'desc', got {direction!r}")
    query.order_direction = direction.lower()  # type: ignore[assignment]
    if query.limit is not None:
        _check_non_negative_int("limit", query.limit)
    _check_non_negative_int("offset", query.offset)


def _check_non_negative_int(name: str, value: object) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{name} must be a non-negative int, got {value!r}")


__all__ = ["validate_query_values"]
