"""
Unit tests for the JSON serialization module.

Tests for:
- EventSourceJSONEncoder class
- json_dumps convenience function
- json_loads convenience function
- Backward compatibility with deprecated imports
- The orjson-backed encoder's contract (non-finite floats, integer range,
  UUID/datetime, subclasses)
"""

import json
from datetime import UTC, datetime
from uuid import uuid4

import pytest

from eventsource.serialization import (
    EventSourceJSONEncoder,
    json_dumps,
    json_loads,
)


class TestEventSourceJSONEncoder:
    """
    Tests for EventSourceJSONEncoder.

    This class is no longer used by json_dumps/json_loads (orjson, now a
    core dependency, serializes UUID/datetime natively) but remains public
    API used directly by eventsource.adapters.postgresql.outbox -- see the class
    docstring in serialization/json.py. These tests exercise it the same
    way outbox.py does: via stdlib json.dumps(..., cls=EventSourceJSONEncoder).
    """

    def test_encodes_uuid(self):
        """Test encoding UUID to string."""
        test_uuid = uuid4()
        result = json.dumps({"id": test_uuid}, cls=EventSourceJSONEncoder)
        assert str(test_uuid) in result

    def test_encodes_datetime(self):
        """Test encoding datetime to ISO format string."""
        now = datetime.now(UTC)
        result = json.dumps({"timestamp": now}, cls=EventSourceJSONEncoder)
        assert now.isoformat() in result

    def test_encodes_datetime_with_microseconds(self):
        """Test encoding datetime with microseconds."""
        test_dt = datetime(2024, 1, 15, 10, 30, 45, 123456, tzinfo=UTC)
        result = json.dumps({"timestamp": test_dt}, cls=EventSourceJSONEncoder)
        assert "123456" in result

    def test_regular_types_unchanged(self):
        """Test that regular types are encoded normally."""
        data = {"string": "hello", "number": 42, "boolean": True}
        result = json.dumps(data, cls=EventSourceJSONEncoder)
        parsed = json.loads(result)
        assert parsed == data

    def test_encode_nested_structure(self):
        """Test encoding nested structures with UUID and datetime."""
        test_uuid = uuid4()
        test_dt = datetime(2024, 1, 15, 10, 30, 45, tzinfo=UTC)

        data = {
            "event": {
                "id": test_uuid,
                "occurred_at": test_dt,
                "items": [{"sub_id": uuid4(), "created_at": datetime.now(UTC)}],
            }
        }

        result = json.dumps(data, cls=EventSourceJSONEncoder)
        parsed = json.loads(result)

        assert parsed["event"]["id"] == str(test_uuid)

    def test_encode_unsupported_type_raises(self):
        """Test that unsupported types raise TypeError."""

        class CustomClass:
            pass

        data = {"custom": CustomClass()}

        with pytest.raises(TypeError):
            json.dumps(data, cls=EventSourceJSONEncoder)


class TestJsonDumps:
    """Tests for json_dumps convenience function."""

    def test_dumps_uuid(self):
        """Test json_dumps with UUID."""
        test_uuid = uuid4()
        result = json_dumps({"id": test_uuid})

        assert str(test_uuid) in result

    def test_dumps_datetime(self):
        """Test json_dumps with datetime."""
        test_dt = datetime(2024, 1, 15, 10, 30, 45, tzinfo=UTC)
        result = json_dumps({"timestamp": test_dt})

        assert "2024-01-15" in result

    def test_dumps_complex_structure(self):
        """Test json_dumps with complex nested structure."""
        data = {
            "event_id": uuid4(),
            "aggregate_id": uuid4(),
            "occurred_at": datetime.now(UTC),
            "payload": {
                "items": [{"id": uuid4()}],
                "metadata": {"created_at": datetime.now(UTC)},
            },
        }

        result = json_dumps(data)

        # Should be valid JSON
        parsed = json.loads(result)
        assert "event_id" in parsed
        assert "payload" in parsed


class TestJsonLoads:
    """Tests for json_loads convenience function."""

    def test_loads_basic(self):
        """Test json_loads with basic JSON string."""
        data = '{"key": "value", "number": 42}'
        result = json_loads(data)

        assert result["key"] == "value"
        assert result["number"] == 42

    def test_loads_roundtrip(self):
        """Test roundtrip with json_dumps and json_loads."""
        original = {
            "id": uuid4(),
            "timestamp": datetime.now(UTC),
            "data": {"nested": "value"},
        }

        json_str = json_dumps(original)
        loaded = json_loads(json_str)

        # UUIDs and datetimes become strings after roundtrip
        assert loaded["id"] == str(original["id"])
        assert loaded["data"]["nested"] == "value"

    def test_loads_array(self):
        """Test json_loads with array."""
        data = '[1, 2, 3, "four"]'
        result = json_loads(data)

        assert result == [1, 2, 3, "four"]


class TestJsonEncoderContract:
    """
    Contract tests for the single orjson-backed encoder.

    History: earlier revisions of this file compared a stdlib fallback
    branch against orjson (a "parity" suite), because orjson was an
    optional extra with a stdlib `json_dumps` fallback. The user has since
    decided orjson is a core dependency and the fallback is deleted -- so
    there is no second branch to compare against anymore. These tests
    instead pin the single encoder's behavior directly: what it accepts,
    what it rejects, and how.

    Two rejections are load-bearing, not incidental:
    - Non-finite floats (inf/-inf/nan) raise ValueError. orjson would
      otherwise silently substitute JSON `null`, which is data corruption
      for a persisted value.
    - Integers outside [-2**63, 2**64-1] raise ValueError with a message
      naming the value and the range. orjson otherwise raises a bare,
      unhelpful `TypeError: Integer exceeds 64-bit range`.

    A UUID dict key serializing successfully (via OPT_NON_STR_KEYS) is
    simply the encoder's behavior now, not a "divergence" -- there is
    nothing left for it to diverge from.
    """

    # -- UUID / datetime -----------------------------------------------

    def test_uuid_serializes(self):
        test_uuid = uuid4()
        result = json_dumps({"id": test_uuid})
        assert json_loads(result) == {"id": str(test_uuid)}

    def test_aware_datetime_serializes(self):
        dt = datetime(2024, 1, 15, 10, 30, 45, 123456, tzinfo=UTC)
        result = json_dumps({"t": dt})
        assert json_loads(result) == {"t": dt.isoformat()}

    def test_naive_datetime_serializes(self):
        dt = datetime(2024, 1, 15, 10, 30, 45, 123456)
        result = json_dumps({"t": dt})
        assert json_loads(result) == {"t": dt.isoformat()}

    def test_uuid_dict_key_serializes(self):
        # orjson, with OPT_NON_STR_KEYS, stringifies a UUID dict key.
        # (stdlib's json.dumps could never do this at all -- there is no
        # fallback left to diverge from, this is just the encoder's
        # behavior.)
        test_uuid = uuid4()
        result = json_dumps({test_uuid: "a"})
        assert str(test_uuid) in result

    def test_round_trip_through_json_loads(self):
        original = {
            "event": {
                "id": uuid4(),
                "occurred_at": datetime(2024, 1, 15, 10, 30, 45, tzinfo=UTC),
                "items": [{"sub_id": uuid4(), "n": 1}, {"sub_id": uuid4(), "n": 2}],
            }
        }
        result = json_dumps(original)
        parsed = json_loads(result)
        assert parsed["event"]["id"] == str(original["event"]["id"])
        assert parsed["event"]["occurred_at"] == original["event"]["occurred_at"].isoformat()
        assert len(parsed["event"]["items"]) == 2

    # -- Structural shapes -----------------------------------------------

    def test_empty_dict(self):
        assert json_dumps({}) == "{}"

    def test_empty_list(self):
        assert json_dumps([]) == "[]"

    def test_none(self):
        assert json_dumps(None) == "null"

    def test_int_dict_key(self):
        assert json_loads(json_dumps({1: "a", 2: "b"})) == {"1": "a", "2": "b"}

    # -- Content dimension: non-ASCII, control chars (still worth pinning) --

    def test_latin1_accented_characters_not_escaped(self):
        result = json_dumps({"name": "Sébastien Müller Peña"})
        assert "Sébastien Müller Peña" in result
        assert "\\u" not in result

    def test_non_latin_script_cjk_not_escaped(self):
        result = json_dumps({"text": "日本語のテキスト"})
        assert "日本語のテキスト" in result
        assert "\\u" not in result

    def test_non_latin_script_cyrillic_not_escaped(self):
        result = json_dumps({"text": "Привет, мир"})
        assert "Привет, мир" in result
        assert "\\u" not in result

    def test_emoji_outside_bmp_not_escaped(self):
        # U+1F600 is outside the Basic Multilingual Plane.
        result = json_dumps({"reaction": "\U0001f600"})
        assert "\U0001f600" in result
        assert "\\u" not in result

    def test_mixed_ascii_and_non_ascii(self):
        text = "Hello, Sébastien! 日本語 \U0001f600 mixed with ASCII."
        result = json_dumps({"msg": text})
        assert json_loads(result) == {"msg": text}

    def test_control_characters(self):
        text = "line1\nline2\ttab\x00null-byte"
        result = json_dumps({"text": text})
        assert json_loads(result) == {"text": text}

    def test_empty_and_whitespace_strings(self):
        data = {"empty": "", "spaces": "   ", "tabs_newlines": "\t\n"}
        assert json_loads(json_dumps(data)) == data

    # -- Finite float / integer values still serialize --------------------

    def test_finite_floats_including_extremes(self):
        data = {
            "zero": 0.0,
            "neg_zero": -0.0,
            "tiny": 5e-300,
            "huge": 1.7e300,
            "precise": 1.123456789012345,
            "negative": -42.5,
        }
        result = json_dumps(data)
        parsed = json_loads(result)
        for key, value in data.items():
            assert parsed[key] == value

    def test_large_integer_within_supported_range(self):
        # Within orjson's u64 range, so this alone would NOT have caught
        # the boundary bug found by the property-testing agent -- kept
        # alongside the boundary tests below rather than in place of them.
        data = {"n": 12345678901234567890}
        assert json_loads(json_dumps(data)) == data

    def test_bool_serializes_as_boolean_not_integer(self):
        result = json_dumps({"flags": [True, False]})
        assert result == '{"flags":[true,false]}'

    # -- Non-finite floats: NOT rejected by json_dumps (round 5) -----------
    #
    # Earlier rounds of this task had json_dumps itself scan for and reject
    # non-finite floats. That scan made the wrapper 14.5x slower than raw
    # orjson.dumps and, on a realistic payload, slower than the stdlib
    # encoder orjson replaced -- defeating the point of the dependency.
    # Round 5 deleted the scan and moved rejection upstream instead: to
    # DomainEvent.model_config (allow_inf_nan=False), which rejects at
    # construction time for every event, before serialization is ever
    # reached. See TestDomainEventRejectsNonFiniteFloats below.
    #
    # The tradeoff, accepted deliberately and documented in
    # docs/reference/serialization-limits.md and the module docstring: a
    # non-finite float in a payload that does NOT go through DomainEvent
    # validation (a hand-built dict, DLQ metadata) is no longer rejected by
    # json_dumps. These tests pin the new, explicit behavior -- silent
    # null -- rather than silently forgetting it was ever a rejection.

    @pytest.mark.parametrize("value", [float("inf"), float("-inf"), float("nan")])
    def test_non_finite_float_in_plain_dict_serializes_as_null(self, value):
        # Documents the residual risk directly: json_dumps no longer
        # raises for this case. This is NOT a bug -- it's the accepted
        # tradeoff. If this test starts failing because json_dumps raises
        # again, that's an intentional design reversion, not a regression
        # to "fix" by re-adding the scan.
        result = json_dumps({"v": value})
        assert result == '{"v":null}'

    def test_non_finite_float_nested_in_list_serializes_as_null(self):
        data = {"items": [1.0, 2.0, {"nested": [float("nan")]}]}
        result = json_dumps(data)
        assert json_loads(result) == {"items": [1.0, 2.0, {"nested": [None]}]}

    def test_non_finite_float_nested_in_dict_serializes_as_null(self):
        data = {"outer": {"inner": {"deep": float("inf")}}}
        result = json_dumps(data)
        assert json_loads(result) == {"outer": {"inner": {"deep": None}}}

    def test_non_finite_float_inside_dict_subclass_serializes_as_null(self):
        class MyDict(dict):
            pass

        data = {"a": MyDict({"x": float("inf")})}
        assert json_loads(json_dumps(data)) == {"a": {"x": None}}

    def test_non_finite_float_inside_list_subclass_serializes_as_null(self):
        class MyList(list):
            pass

        data = {"a": MyList([float("nan")])}
        assert json_loads(json_dumps(data)) == {"a": [None]}

    def test_tuple_subclass_is_unsupported_regardless_of_content(self):
        # Not actually about non-finite floats: found while updating this
        # test for round 5. orjson does not serialize `tuple` *subclasses*
        # natively at all (unlike `list`/`dict` subclasses, which it does)
        # -- confirmed by direct execution with plain finite content, no
        # non-finite float involved. This was invisible in earlier rounds
        # because the (now-deleted) pre-scan raised ValueError for the
        # non-finite float inside before orjson.dumps ever got a chance to
        # reject the tuple subclass itself for an unrelated reason.
        class MyTuple(tuple):
            pass

        with pytest.raises(TypeError, match="not JSON serializable"):
            json_dumps({"a": MyTuple([1, "finite", 2.5])})

    # -- Integer range: rejected -----------------------------------------
    #
    # orjson only supports integers within [-2**63, 2**64-1]. Boundary
    # values, both sides of both limits, found by direct execution (and
    # independently by the property-testing agent's Hypothesis run).

    def test_int_min_boundary_ok(self):
        value = -(2**63)
        assert json_loads(json_dumps({"n": value})) == {"n": value}

    def test_int_below_min_boundary_raises(self):
        value = -(2**63) - 1
        with pytest.raises(ValueError, match="Integer out of range"):
            json_dumps({"n": value})

    def test_out_of_range_int_error_message_matches_documented_text(self):
        """
        docs/reference/serialization-limits.md quotes this message verbatim
        for users. A `match=` substring check (used by the other boundary
        tests above) would still pass if a typo were introduced anywhere
        except the substring itself, so this test asserts full equality
        against the documented string.
        """
        with pytest.raises(ValueError) as exc_info:
            json_dumps({"n": 2**64})
        assert (
            str(exc_info.value)
            == "Integer out of range for JSON serialization (must be within [-2**63, 2**64-1])"
        )

    def test_int_max_boundary_ok(self):
        value = 2**64 - 1
        assert json_loads(json_dumps({"n": value})) == {"n": value}

    def test_int_above_max_boundary_raises(self):
        value = 2**64
        with pytest.raises(ValueError, match="Integer out of range"):
            json_dumps({"n": value})

    def test_out_of_range_int_nested_in_dict_raises(self):
        data = {"outer": {"inner": 2**64}}
        with pytest.raises(ValueError, match="Integer out of range"):
            json_dumps(data)

    def test_out_of_range_int_nested_in_list_raises(self):
        data = {"items": [1, 2, [2**64]]}
        with pytest.raises(ValueError, match="Integer out of range"):
            json_dumps(data)

    def test_out_of_range_int_inside_dict_subclass_raises(self):
        class MyDict(dict):
            pass

        data = {"a": MyDict({"x": 2**64})}
        with pytest.raises(ValueError, match="Integer out of range"):
            json_dumps(data)

    def test_out_of_range_int_inside_list_subclass_raises(self):
        class MyList(list):
            pass

        data = {"a": MyList([-(2**63) - 1])}
        with pytest.raises(ValueError, match="Integer out of range"):
            json_dumps(data)

    def test_bool_never_range_checked(self):
        # bool is an int subclass; isinstance(True, int) is True. It must
        # never be treated as a (trivially in-range) integer value subject
        # to range checking -- it must serialize as true/false, always,
        # regardless of the numeric range logic.
        assert json_dumps({"a": True, "b": False}) == '{"a":true,"b":false}'

    def test_int_subclass_in_range_serializes(self):
        from enum import IntEnum

        class Color(IntEnum):
            RED = 1

        assert json_loads(json_dumps({"c": Color.RED})) == {"c": 1}

    def test_int_subclass_out_of_range_raises(self):
        class MyInt(int):
            pass

        with pytest.raises(ValueError, match="Integer out of range"):
            json_dumps({"n": MyInt(2**64)})

    # -- float subclasses (round 5: same null-on-non-finite behavior as
    # plain floats, not a raise -- see _orjson_default's docstring for why
    # a raise-based design was tried and found not to work: orjson
    # swallows any exception raised inside default=, so there was never a
    # way to signal "non-finite" specifically through that hook) ---------

    def test_float_subclass_finite_serializes(self):
        class MyFloat(float):
            pass

        result = json_dumps({"v": MyFloat(3.14)})
        assert json_loads(result) == {"v": 3.14}

    @pytest.mark.parametrize(
        "value",
        [float("inf"), float("-inf"), float("nan")],
    )
    def test_float_subclass_non_finite_serializes_as_null(self, value):
        class MyFloat(float):
            pass

        result = json_dumps({"v": MyFloat(value)})
        assert result == '{"v":null}'

    def test_nested_float_subclass_in_dict_subclass_in_list_serializes_as_null(self):
        # Combination case: a float subclass holding infinity, inside a
        # dict subclass, inside a plain list.
        class MyDict(dict):
            pass

        class MyFloat(float):
            pass

        data = {"outer": [MyDict({"inner": MyFloat(float("inf"))})]}
        assert json_loads(json_dumps(data)) == {"outer": [{"inner": None}]}

    # -- int / str subclasses: verified, not assumed -----------------------

    def test_int_subclass_serializes(self):
        class MyInt(int):
            pass

        assert json_loads(json_dumps({"v": MyInt(42)})) == {"v": 42}

    def test_str_subclass_serializes(self):
        class MyStr(str):
            pass

        assert json_loads(json_dumps({"v": MyStr("hi")})) == {"v": "hi"}


class TestDomainEventRejectsNonFiniteFloats:
    """
    Round 5: non-finite float rejection moved from json_dumps (a
    pre-serialization scan, deleted for the throughput hit it caused) to
    DomainEvent.model_config (allow_inf_nan=False in
    src/eventsource/events/base.py). This is the real fix for the main
    event path: the outbox writes via
    json.loads(event.model_dump_json()) (outbox.py:264, 557, 837), and
    pydantic's own model_dump_json() already nulls a non-finite float
    silently -- so the old json_dumps-level scan never actually protected
    that path at all; it only ever covered hand-built dicts. Rejecting at
    construction, before pydantic's own serialization ever runs, is
    earlier and covers every event, not just ones passed through
    json_dumps directly.
    """

    def test_domain_event_rejects_inf_at_construction(self):
        from pydantic import ValidationError

        from eventsource.events.base import DomainEvent

        class MyEvent(DomainEvent):
            value: float

        with pytest.raises(ValidationError, match="finite_number|finite"):
            MyEvent(
                aggregate_id=uuid4(),
                aggregate_type="Test",
                value=float("inf"),
            )

    def test_domain_event_rejects_neg_inf_at_construction(self):
        from pydantic import ValidationError

        from eventsource.events.base import DomainEvent

        class MyEvent(DomainEvent):
            value: float

        with pytest.raises(ValidationError, match="finite_number|finite"):
            MyEvent(
                aggregate_id=uuid4(),
                aggregate_type="Test",
                value=float("-inf"),
            )

    def test_domain_event_rejects_nan_at_construction(self):
        from pydantic import ValidationError

        from eventsource.events.base import DomainEvent

        class MyEvent(DomainEvent):
            value: float

        with pytest.raises(ValidationError, match="finite_number|finite"):
            MyEvent(
                aggregate_id=uuid4(),
                aggregate_type="Test",
                value=float("nan"),
            )

    def test_domain_event_accepts_finite_floats(self):
        from eventsource.events.base import DomainEvent

        class MyEvent(DomainEvent):
            value: float

        event = MyEvent(aggregate_id=uuid4(), aggregate_type="Test", value=3.14)
        assert event.value == 3.14

    def test_domain_event_rejects_non_finite_float_nested_in_payload_field(self):
        # Pydantic's allow_inf_nan applies recursively through model
        # fields, not just top-level scalar fields.
        from pydantic import ValidationError

        from eventsource.events.base import DomainEvent

        class MyEvent(DomainEvent):
            amounts: list[float]

        with pytest.raises(ValidationError, match="finite_number|finite"):
            MyEvent(
                aggregate_id=uuid4(),
                aggregate_type="Test",
                amounts=[1.0, 2.0, float("nan")],
            )

    def test_domain_event_still_frozen(self):
        # allow_inf_nan=False must not have displaced frozen=True in
        # model_config -- both are set together.
        from eventsource.events.base import DomainEvent

        class MyEvent(DomainEvent):
            value: float

        event = MyEvent(aggregate_id=uuid4(), aggregate_type="Test", value=1.0)
        with pytest.raises(Exception):  # noqa: B017 - pydantic's ValidationError for frozen models
            event.value = 2.0


class TestNewModuleExports:
    """Tests to verify the new module structure works correctly."""

    def test_import_from_serialization_module(self):
        """Test importing from eventsource.serialization works."""
        from eventsource.serialization import (
            EventSourceJSONEncoder,
            json_dumps,
            json_loads,
        )

        # Verify functions are callable
        result = json_dumps({"id": uuid4()})
        assert isinstance(result, str)

        parsed = json_loads(result)
        assert isinstance(parsed, dict)

        # Verify encoder is a class
        assert isinstance(EventSourceJSONEncoder, type)

    def test_import_from_serialization_json_submodule(self):
        """Test importing from eventsource.serialization.json works."""
        from eventsource.serialization.json import (
            EventSourceJSONEncoder,
            json_dumps,
            json_loads,
        )

        # Verify functions are callable
        result = json_dumps({"id": uuid4()})
        assert isinstance(result, str)

        parsed = json_loads(result)
        assert isinstance(parsed, dict)

        # Verify encoder is a class
        assert isinstance(EventSourceJSONEncoder, type)

    def test_encoder_exported_from_top_level(self):
        """Test that EventSourceJSONEncoder is exported from eventsource package."""
        from eventsource import EventSourceJSONEncoder

        # Verify encoder is a class
        assert isinstance(EventSourceJSONEncoder, type)

        # Verify it works
        result = json.dumps({"id": uuid4()}, cls=EventSourceJSONEncoder)
        assert isinstance(result, str)
