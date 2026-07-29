"""
Property-based tests for `eventsource.serialization.json`.

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
- Non-finite floats (`inf`, `-inf`, `nan`), generated deliberately rather
  than excluded, must always raise `ValueError` -- see
  `_reject_non_finite_floats` in `src/eventsource/serialization/json.py`.

Excluded from the JSON-native payload strategy: non-finite floats (covered
by their own dedicated property instead) and non-`str` dict keys (the
"valid JSON" property parses with stdlib `json.loads`, which has no way to
represent a non-string key in the first place -- a strategy-shape concern,
not a divergence being dodged).
"""

from __future__ import annotations

import json as stdlib_json

import pytest
from hypothesis import given
from hypothesis import strategies as st

from eventsource.serialization import json_dumps, json_loads

json_values = st.recursive(
    st.none()
    | st.booleans()
    # Bounded to orjson's supported integer range [-2**63, 2**64 - 1].
    # Outside that range orjson.dumps raises `TypeError: Integer exceeds
    # 64-bit range` while stdlib handles arbitrary precision -- a real,
    # previously undocumented divergence found by this property (see
    # task-2c-report.md). Reported upstream and awaiting a decision on
    # whether to fix the encoder or document the limit; bounding here for
    # now so the rest of this property can make progress in the meantime.
    | st.integers(min_value=-(2**63), max_value=2**64 - 1)
    | st.floats(allow_nan=False, allow_infinity=False)
    | st.text(),
    lambda children: st.lists(children) | st.dictionaries(st.text(), children),
    max_leaves=20,
)

non_finite_floats = st.sampled_from([float("inf"), float("-inf"), float("nan")])


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
def test_non_finite_floats_raise(value: float) -> None:
    """inf, -inf, and nan must always raise ValueError, never serialize."""
    with pytest.raises(ValueError):
        json_dumps(value)
