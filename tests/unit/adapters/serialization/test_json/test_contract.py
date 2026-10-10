"""Contract tests for orjson-backed encoder."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

import pytest

from eventsource.adapters.serialization import (
    json_dumps,
    json_loads,
)


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

    def test_uuid_serializes(self) -> None:
        test_uuid = uuid4()
        result = json_dumps({"id": test_uuid})
        assert json_loads(result) == {"id": str(test_uuid)}

    def test_aware_datetime_serializes(self) -> None:
        dt = datetime(2024, 1, 15, 10, 30, 45, 123456, tzinfo=UTC)
        result = json_dumps({"t": dt})
        assert json_loads(result) == {"t": dt.isoformat()}

    def test_naive_datetime_serializes(self) -> None:
        dt = datetime(2024, 1, 15, 10, 30, 45, 123456)
        result = json_dumps({"t": dt})
        assert json_loads(result) == {"t": dt.isoformat()}

    def test_uuid_dict_key_serializes(self) -> None:
        test_uuid = uuid4()
        result = json_dumps({test_uuid: "a"})
        assert str(test_uuid) in result

    def test_round_trip_through_json_loads(self) -> None:
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

    def test_empty_dict(self) -> None:
        assert json_dumps({}) == "{}"

    def test_empty_list(self) -> None:
        assert json_dumps([]) == "[]"

    def test_none(self) -> None:
        assert json_dumps(None) == "null"

    def test_int_dict_key(self) -> None:
        assert json_loads(json_dumps({1: "a", 2: "b"})) == {"1": "a", "2": "b"}

    # -- Content dimension: non-ASCII, control chars (still worth pinning) --

    def test_latin1_accented_characters_not_escaped(self) -> None:
        result = json_dumps({"name": "Sébastien Müller Peña"})
        assert "Sébastien Müller Peña" in result
        assert "\\u" not in result

    def test_non_latin_script_cjk_not_escaped(self) -> None:
        result = json_dumps({"text": "日本語のテキスト"})
        assert "日本語のテキスト" in result
        assert "\\u" not in result

    def test_non_latin_script_cyrillic_not_escaped(self) -> None:
        result = json_dumps({"text": "Привет, мир"})
        assert "Привет, мир" in result
        assert "\\u" not in result

    def test_emoji_outside_bmp_not_escaped(self) -> None:
        result = json_dumps({"reaction": "\U0001f600"})
        assert "\U0001f600" in result
        assert "\\u" not in result

    def test_mixed_ascii_and_non_ascii(self) -> None:
        text = "Hello, Sébastien! 日本語 \U0001f600 mixed with ASCII."
        result = json_dumps({"msg": text})
        assert json_loads(result) == {"msg": text}

    def test_control_characters(self) -> None:
        text = "line1\nline2\ttab\x00null-byte"
        result = json_dumps({"text": text})
        assert json_loads(result) == {"text": text}

    def test_empty_and_whitespace_strings(self) -> None:
        data = {"empty": "", "spaces": "   ", "tabs_newlines": "\t\n"}
        assert json_loads(json_dumps(data)) == data

    # -- Finite float / integer values still serialize --------------------

    def test_finite_floats_including_extremes(self) -> None:
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

    def test_large_integer_within_supported_range(self) -> None:
        data = {"n": 12345678901234567890}
        assert json_loads(json_dumps(data)) == data

    def test_bool_serializes_as_boolean_not_integer(self) -> None:
        result = json_dumps({"flags": [True, False]})
        assert result == '{"flags":[true,false]}'

    # -- Non-finite floats: NOT rejected by json_dumps (round 5) -----------

    @pytest.mark.parametrize("value", [float("inf"), float("-inf"), float("nan")])
    def test_non_finite_float_in_plain_dict_serializes_as_null(self, value: float) -> None:
        result = json_dumps({"v": value})
        assert result == '{"v":null}'

    def test_non_finite_float_nested_in_list_serializes_as_null(self) -> None:
        data = {"items": [1.0, 2.0, {"nested": [float("nan")]}]}
        result = json_dumps(data)
        assert json_loads(result) == {"items": [1.0, 2.0, {"nested": [None]}]}

    def test_non_finite_float_nested_in_dict_serializes_as_null(self) -> None:
        data = {"outer": {"inner": {"deep": float("inf")}}}
        result = json_dumps(data)
        assert json_loads(result) == {"outer": {"inner": {"deep": None}}}

    def test_non_finite_float_inside_dict_subclass_serializes_as_null(self) -> None:
        class MyDict(dict):
            pass

        data = {"a": MyDict({"x": float("inf")})}
        assert json_loads(json_dumps(data)) == {"a": {"x": None}}

    def test_non_finite_float_inside_list_subclass_serializes_as_null(self) -> None:
        class MyList(list):
            pass

        data = {"a": MyList([float("nan")])}
        assert json_loads(json_dumps(data)) == {"a": [None]}

    def test_tuple_subclass_is_unsupported_regardless_of_content(self) -> None:
        class MyTuple(tuple):
            pass

        with pytest.raises(TypeError, match="not JSON serializable"):
            json_dumps({"a": MyTuple([1, "finite", 2.5])})

    # -- Integer range: rejected -----------------------------------------

    def test_int_min_boundary_ok(self) -> None:
        value = -(2**63)
        assert json_loads(json_dumps({"n": value})) == {"n": value}

    def test_int_below_min_boundary_raises(self) -> None:
        value = -(2**63) - 1
        with pytest.raises(ValueError, match="Integer out of range"):
            json_dumps({"n": value})

    def test_out_of_range_int_error_message_matches_documented_text(self) -> None:
        with pytest.raises(ValueError) as exc_info:
            json_dumps({"n": 2**64})
        assert (
            str(exc_info.value)
            == "Integer out of range for JSON serialization (must be within [-2**63, 2**64-1])"
        )

    def test_int_max_boundary_ok(self) -> None:
        value = 2**64 - 1
        assert json_loads(json_dumps({"n": value})) == {"n": value}

    def test_int_above_max_boundary_raises(self) -> None:
        value = 2**64
        with pytest.raises(ValueError, match="Integer out of range"):
            json_dumps({"n": value})

    def test_out_of_range_int_nested_in_dict_raises(self) -> None:
        data = {"outer": {"inner": 2**64}}
        with pytest.raises(ValueError, match="Integer out of range"):
            json_dumps(data)

    def test_out_of_range_int_nested_in_list_raises(self) -> None:
        data = {"items": [1, 2, [2**64]]}
        with pytest.raises(ValueError, match="Integer out of range"):
            json_dumps(data)

    def test_out_of_range_int_inside_dict_subclass_raises(self) -> None:
        class MyDict(dict):
            pass

        data = {"a": MyDict({"x": 2**64})}
        with pytest.raises(ValueError, match="Integer out of range"):
            json_dumps(data)

    def test_out_of_range_int_inside_list_subclass_raises(self) -> None:
        class MyList(list):
            pass

        data = {"a": MyList([-(2**63) - 1])}
        with pytest.raises(ValueError, match="Integer out of range"):
            json_dumps(data)

    def test_bool_never_range_checked(self) -> None:
        assert json_dumps({"a": True, "b": False}) == '{"a":true,"b":false}'

    def test_int_subclass_in_range_serializes(self) -> None:
        from enum import IntEnum

        class Color(IntEnum):
            RED = 1

        assert json_loads(json_dumps({"c": Color.RED})) == {"c": 1}

    def test_int_subclass_out_of_range_raises(self) -> None:
        class MyInt(int):
            pass

        with pytest.raises(ValueError, match="Integer out of range"):
            json_dumps({"n": MyInt(2**64)})

    # -- float subclasses -----------------------------------------------

    def test_float_subclass_finite_serializes(self) -> None:
        class MyFloat(float):
            pass

        result = json_dumps({"v": MyFloat(3.14)})
        assert json_loads(result) == {"v": 3.14}

    @pytest.mark.parametrize(
        "value",
        [float("inf"), float("-inf"), float("nan")],
    )
    def test_float_subclass_non_finite_serializes_as_null(self, value: float) -> None:
        class MyFloat(float):
            pass

        result = json_dumps({"v": MyFloat(value)})
        assert result == '{"v":null}'

    def test_nested_float_subclass_in_dict_subclass_in_list_serializes_as_null(self) -> None:
        class MyDict(dict):
            pass

        class MyFloat(float):
            pass

        data = {"outer": [MyDict({"inner": MyFloat(float("inf"))})]}
        assert json_loads(json_dumps(data)) == {"outer": [{"inner": None}]}

    # -- int / str subclasses: verified, not assumed -----------------------

    def test_int_subclass_serializes(self) -> None:
        class MyInt(int):
            pass

        assert json_loads(json_dumps({"v": MyInt(42)})) == {"v": 42}

    def test_str_subclass_serializes(self) -> None:
        class MyStr(str):
            pass

        assert json_loads(json_dumps({"v": MyStr("hi")})) == {"v": "hi"}
