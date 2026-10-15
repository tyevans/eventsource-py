"""Property-based tests for `eventsource.adapters.serialization.json`.

Covers round-trip serialization, stdlib `json.loads` validity, non-finite float
handling (serialized as `null` in plain dicts, rejected at `DomainEvent`
construction via `allow_inf_nan=False`), 64-bit integer range enforcement
(`[-2**63, 2**64 - 1]`), and deep nesting behavior.
"""

from __future__ import annotations

import json as stdlib_json
from uuid import uuid4

import pytest
from hypothesis import given
from hypothesis import strategies as st
from pydantic import ValidationError

from eventsource.adapters.serialization import json_dumps, json_loads
from eventsource.domain.event import DomainEvent

json_values = st.recursive(
    st.none()
    | st.booleans()
    | st.integers(min_value=-(2**63), max_value=2**64 - 1)
    | st.floats(allow_nan=False, allow_infinity=False)
    | st.text(),
    lambda children: st.lists(children) | st.dictionaries(st.text(), children),
    max_leaves=20,
)

non_finite_floats = st.sampled_from([float("inf"), float("-inf"), float("nan")])


class _EventWithFloat(DomainEvent):
    """Minimal DomainEvent subclass for exercising `allow_inf_nan=False`."""

    aggregate_type: str = "Test"
    value: float


out_of_range_ints = st.integers(max_value=-(2**63) - 1) | st.integers(min_value=2**64)

deeply_nested_values = st.recursive(
    st.none()
    | st.booleans()
    | st.integers(min_value=-(2**63), max_value=2**64 - 1)
    | st.text(max_size=5),
    lambda children: st.lists(children, min_size=1, max_size=1)
    | st.dictionaries(st.text(max_size=3), children, min_size=1, max_size=1),
    max_leaves=200,
)


@given(payload=json_values)
def test_json_roundtrip(payload: object) -> None:
    """`json_loads(json_dumps(x)) == x` for JSON-native values."""
    assert json_loads(json_dumps(payload)) == payload


@given(payload=json_values)
def test_json_dumps_returns_str(payload: object) -> None:
    assert isinstance(json_dumps(payload), str)


@given(payload=json_values)
def test_json_dumps_output_is_valid_json(payload: object) -> None:
    """`json_dumps` output must parse as valid JSON under stdlib `json.loads`."""
    stdlib_json.loads(json_dumps(payload))


@given(value=non_finite_floats)
def test_non_finite_float_in_plain_payload_serializes_to_null(value: float) -> None:
    """Non-finite floats in plain payloads serialize to JSON `null` under orjson."""
    assert json_dumps({"v": value}) == '{"v":null}'


@given(value=non_finite_floats)
def test_domain_event_rejects_non_finite_float_at_construction(value: float) -> None:
    """`DomainEvent` subclasses reject `inf`, `-inf`, and `nan` at construction."""
    with pytest.raises(ValidationError):
        _EventWithFloat(aggregate_id=uuid4(), value=value)


@given(value=out_of_range_ints)
def test_out_of_range_int_raises_at_top_level(value: int) -> None:
    """Integers outside `[-2**63, 2**64 - 1]` raise `ValueError`."""
    with pytest.raises(ValueError):
        json_dumps(value)


@given(value=out_of_range_ints)
def test_out_of_range_int_raises_nested_in_dict(value: int) -> None:
    """Same limit, enforced when the out-of-range int is nested in a dict."""
    with pytest.raises(ValueError):
        json_dumps({"n": value})


@given(value=out_of_range_ints)
def test_out_of_range_int_raises_nested_in_list(value: int) -> None:
    """Same limit, enforced when the out-of-range int is nested in a list."""
    with pytest.raises(ValueError):
        json_dumps([value])


@given(payload=deeply_nested_values)
def test_deeply_nested_payload_does_not_diverge(payload: object) -> None:
    """Deeply nested payloads round-trip cleanly without recursion divergence."""
    assert json_loads(json_dumps(payload)) == payload
