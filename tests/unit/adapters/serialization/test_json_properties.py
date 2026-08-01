"""
Property-based tests for `eventsource.adapters.serialization.json`.

These complement (not replace) the hand-enumerated cases in `test_json.py`.
That suite covers UUID, datetime, and a handful of hand-picked non-ASCII
strings; it does not sweep unicode, surrogates, or astral-plane codepoints,
which is exactly the class of gap Hypothesis generates for free. See
`.superpowers/sdd/2026-07-28-m0-sqlalchemy-unification/task-2c-brief.md`
for the rationale.

NOTE on scope: the brief's original Step 2 called for a parity property
toggling `ORJSON_AVAILABLE` to compare the orjson and stdlib-fallback
branches of `json_dumps`. That property is deliberately NOT present here.
Per a direction change mid-task, orjson is becoming a hard dependency and
the stdlib fallback (and the `ORJSON_AVAILABLE` flag it hangs off) is being
removed entirely. A property built around a flag that's going away would
either fail outright once it's gone or, worse, silently pass vacuously
(toggling a flag nothing branches on anymore) -- exactly the class of
useless-but-green test this milestone is trying to eliminate. See
task-2c-report.md for the full account.

What's covered instead, against the single (orjson-backed) encoder:

- Round-trip: `json_loads(json_dumps(x)) == x` for JSON-native values.
- Contract: `json_dumps` always returns `str`, and that `str` always parses
  as valid JSON via stdlib `json.loads` -- an independent decoder from the
  library's own `json_loads`, so this can't pass by both sides sharing the
  same bug.
- Non-finite floats (`inf`, `-inf`, `nan`) in a plain (non-`DomainEvent`)
  payload, generated deliberately rather than excluded, serialize to JSON
  `null` -- this is orjson's own behavior, deliberately left unmodified
  (see `docs/reference/serialization-limits.md`). An earlier version of
  this encoder pre-scanned every payload to reject these with `ValueError`
  instead, but that scan made `json_dumps` 14.5x slower than raw
  `orjson.dumps` -- slower than the stdlib encoder orjson was adopted to
  replace -- while protecting against an input class the outbox's actual
  path (`DomainEvent.model_dump_json()`) never sees, since pydantic's own
  serializer already nulls non-finite floats before `json_dumps` runs. The
  scan was deleted; see the property for `DomainEvent` construction below
  for where the real protection now lives.
- A `DomainEvent` subclass with a float field must reject `inf`, `-inf`,
  and `nan` at *construction* time with `pydantic.ValidationError` --
  `DomainEvent.model_config` sets `allow_inf_nan=False`. This is the
  property that actually protects the invariant (every event in the
  system, at the earliest possible point), unlike the deleted
  `json_dumps`-level scan it replaces.
- Integer range limit: orjson (now the sole encoder) only supports
  `[-2**63, 2**64 - 1]`; this is a documented hard limit of the chosen
  encoder, not a divergence to unify. `json_values` below is bounded to
  that range for the general properties. `test_out_of_range_int_raises`
  asserts the limit is *enforced* (raises `ValueError`, not orjson's bare
  `TypeError`) for values outside it. This property was written while the
  range guard was still in flight on a concurrent task and initially
  failed against pre-guard code (as intended -- see task-2c-report.md for
  the counterexample); the guard has since landed in `json_dumps` /
  `_validate_json_safe_values` and the property now passes.
- Deep nesting: the encoder must not diverge in behavior or blow a
  recursion limit at depth. `_reject_non_finite_floats`'s pre-scan is an
  iterative stack walk (no recursion-depth limit of its own) while
  `orjson.dumps` recurses internally (C stack, its own limit) -- their
  depth limits are not guaranteed to agree, so this is tested directly
  rather than assumed.

Excluded from the JSON-native payload strategy: non-finite floats (covered
by their own dedicated property instead) and non-`str` dict keys (the
"valid JSON" property parses with stdlib `json.loads`, which has no way to
represent a non-string key in the first place -- a strategy-shape concern,
not a divergence being dodged).
"""

from __future__ import annotations

import json as stdlib_json
from uuid import uuid4

import pytest
from hypothesis import given
from hypothesis import strategies as st
from pydantic import ValidationError

from eventsource.adapters.serialization import json_dumps, json_loads
from eventsource.events.base import DomainEvent

json_values = st.recursive(
    st.none()
    | st.booleans()
    # Bounded to orjson's supported integer range [-2**63, 2**64 - 1] --
    # the encoder's documented supported range (see `json_dumps` /
    # `_validate_json_safe_values` in serialization/json.py), which raises
    # ValueError outside it. Values outside the range are exercised
    # separately by `test_out_of_range_int_raises` below, not mixed into
    # this general-purpose strategy.
    | st.integers(min_value=-(2**63), max_value=2**64 - 1)
    | st.floats(allow_nan=False, allow_infinity=False)
    | st.text(),
    lambda children: st.lists(children) | st.dictionaries(st.text(), children),
    max_leaves=20,
)

non_finite_floats = st.sampled_from([float("inf"), float("-inf"), float("nan")])
# `sampled_from` here is the complete enumeration, not a shortcut standing in
# for a wider Hypothesis strategy: `math.isfinite` has exactly three false
# cases in IEEE-754 double precision as Python's `float` exposes it --
# +inf, -inf, and nan -- there is no fourth non-finite value or NaN payload
# variant reachable through the `float` type to generate toward.


class _EventWithFloat(DomainEvent):
    """Minimal DomainEvent subclass for exercising `allow_inf_nan=False`."""

    aggregate_type: str = "Test"
    value: float


out_of_range_ints = st.integers(max_value=-(2**63) - 1) | st.integers(min_value=2**64)

# Deep, narrow (mostly single-child) structures to probe recursion depth
# specifically, rather than breadth -- `max_leaves` alone tends to produce
# wide-ish trees, not deep chains.
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
    """
    `json_dumps` output must parse as valid JSON under an independent
    decoder (stdlib `json.loads`, not this library's own `json_loads`).
    """
    stdlib_json.loads(json_dumps(payload))


@given(value=non_finite_floats)
def test_non_finite_float_in_plain_payload_serializes_to_null(value: float) -> None:
    """
    A non-finite float in a plain (non-`DomainEvent`) payload serializes to
    JSON `null` -- this is documented, deliberately-accepted orjson
    behavior (see `docs/reference/serialization-limits.md`), not a
    regression. `json_dumps` no longer pre-scans for and rejects these; the
    real protection is at `DomainEvent` construction time instead, see
    `test_domain_event_rejects_non_finite_float_at_construction` below.
    """
    assert json_dumps({"v": value}) == '{"v":null}'


@given(value=non_finite_floats)
def test_domain_event_rejects_non_finite_float_at_construction(value: float) -> None:
    """
    A `DomainEvent` subclass with a float field must reject `inf`, `-inf`,
    and `nan` with `pydantic.ValidationError` at construction -- before
    serialization is ever reached. This is where non-finite-float
    protection actually lives now (`DomainEvent.model_config` sets
    `allow_inf_nan=False`); see
    `docs/reference/serialization-limits.md`.
    """
    with pytest.raises(ValidationError):
        _EventWithFloat(aggregate_id=uuid4(), value=value)


@given(value=out_of_range_ints)
def test_out_of_range_int_raises_at_top_level(value: int) -> None:
    """
    Integers outside orjson's supported range ([-2**63, 2**64 - 1]) must
    raise ValueError with the offending value identified -- the same
    documented-rejection pattern as non-finite floats -- rather than
    orjson's undocumented bare `TypeError: Integer exceeds 64-bit range`.
    See task-2c-report.md for the counterexample this property found
    against pre-guard code.
    """
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
    """
    Deep nesting must not blow a recursion limit, and if it does, both the
    non-finite pre-scan (iterative) and orjson's own C-stack recursion must
    fail or succeed together rather than one silently accepting a structure
    the other can't handle. Round-trips like the shallow case; a
    `RecursionError` here (from either side) is itself the finding.
    """
    assert json_loads(json_dumps(payload)) == payload
