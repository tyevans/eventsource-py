"""Contract tests for orjson-backed encoder."""

from __future__ import annotations

from datetime import UTC, datetime
from enum import IntEnum
from uuid import uuid4

import pytest

from eventsource.adapters.serialization import json_dumps, json_loads


class _MyDict(dict):
    pass


class _MyList(list):
    pass


class _MyTuple(tuple):
    pass


class _MyInt(int):
    pass


class _MyFloat(float):
    pass


class _MyStr(str):
    pass


class _Color(IntEnum):
    RED = 1


class TestJsonEncoderContract:
    def test_uuid_serializes(self) -> None:
        test_uuid = uuid4()
        assert json_loads(json_dumps({"id": test_uuid})) == {"id": str(test_uuid)}

    def test_aware_datetime_serializes(self) -> None:
        dt = datetime(2024, 1, 15, 10, 30, 45, 123456, tzinfo=UTC)
        assert json_loads(json_dumps({"t": dt})) == {"t": dt.isoformat()}

    def test_naive_datetime_serializes(self) -> None:
        dt = datetime(2024, 1, 15, 10, 30, 45, 123456)
        assert json_loads(json_dumps({"t": dt})) == {"t": dt.isoformat()}

    def test_uuid_dict_key_serializes(self) -> None:
        test_uuid = uuid4()
        assert str(test_uuid) in json_dumps({test_uuid: "a"})

    def test_round_trip_through_json_loads(self) -> None:
        original = {
            "event": {
                "id": uuid4(),
                "occurred_at": datetime(2024, 1, 15, 10, 30, 45, tzinfo=UTC),
                "items": [{"sub_id": uuid4(), "n": 1}, {"sub_id": uuid4(), "n": 2}],
            }
        }
        parsed = json_loads(json_dumps(original))
        assert parsed["event"]["id"] == str(original["event"]["id"])
        assert parsed["event"]["occurred_at"] == original["event"]["occurred_at"].isoformat()
        assert len(parsed["event"]["items"]) == 2

    def test_empty_dict(self) -> None:
        assert json_dumps({}) == "{}"

    def test_empty_list(self) -> None:
        assert json_dumps([]) == "[]"

    def test_none(self) -> None:
        assert json_dumps(None) == "null"

    def test_int_dict_key(self) -> None:
        assert json_loads(json_dumps({1: "a", 2: "b"})) == {"1": "a", "2": "b"}

    def test_latin1_accented_characters_not_escaped(self) -> None:
        result = json_dumps({"name": "Sébastien Müller Peña"})
        assert "Sébastien Müller Peña" in result and "\\u" not in result

    def test_non_latin_script_cjk_not_escaped(self) -> None:
        result = json_dumps({"text": "日本語のテキスト"})
        assert "日本語のテキスト" in result and "\\u" not in result

    def test_non_latin_script_cyrillic_not_escaped(self) -> None:
        result = json_dumps({"text": "Привет, мир"})
        assert "Привет, мир" in result and "\\u" not in result

    def test_emoji_outside_bmp_not_escaped(self) -> None:
        result = json_dumps({"reaction": "\U0001f600"})
        assert "\U0001f600" in result and "\\u" not in result

    def test_mixed_ascii_and_non_ascii(self) -> None:
        text = "Hello, Sébastien! 日本語 \U0001f600 mixed with ASCII."
        assert json_loads(json_dumps({"msg": text})) == {"msg": text}

    def test_control_characters(self) -> None:
        text = "line1\nline2\ttab\x00null-byte"
        assert json_loads(json_dumps({"text": text})) == {"text": text}

    def test_empty_and_whitespace_strings(self) -> None:
        data = {"empty": "", "spaces": "   ", "tabs_newlines": "\t\n"}
        assert json_loads(json_dumps(data)) == data

    def test_finite_floats_including_extremes(self) -> None:
        data = {"zero": 0.0, "neg_zero": -0.0, "tiny": 5e-300, "huge": 1.7e300, "precise": 1.123456789012345, "negative": -42.5}  # fmt: skip
        assert json_loads(json_dumps(data)) == data

    def test_large_integer_within_supported_range(self) -> None:
        assert json_loads(json_dumps({"n": 12345678901234567890})) == {"n": 12345678901234567890}

    def test_bool_serializes_as_boolean_not_integer(self) -> None:
        assert json_dumps({"flags": [True, False]}) == '{"flags":[true,false]}'

    @pytest.mark.parametrize("value", [float("inf"), float("-inf"), float("nan")])
    def test_non_finite_float_in_plain_dict_serializes_as_null(self, value: float) -> None:
        assert json_dumps({"v": value}) == '{"v":null}'

    def test_non_finite_float_nested_in_list_serializes_as_null(self) -> None:
        data = {"items": [1.0, 2.0, {"nested": [float("nan")]}]}
        assert json_loads(json_dumps(data)) == {"items": [1.0, 2.0, {"nested": [None]}]}

    def test_non_finite_float_nested_in_dict_serializes_as_null(self) -> None:
        data = {"outer": {"inner": {"deep": float("inf")}}}
        assert json_loads(json_dumps(data)) == {"outer": {"inner": {"deep": None}}}

    def test_non_finite_float_inside_dict_subclass_serializes_as_null(self) -> None:
        assert json_loads(json_dumps({"a": _MyDict({"x": float("inf")})})) == {"a": {"x": None}}

    def test_non_finite_float_inside_list_subclass_serializes_as_null(self) -> None:
        assert json_loads(json_dumps({"a": _MyList([float("nan")])})) == {"a": [None]}

    def test_tuple_subclass_is_unsupported_regardless_of_content(self) -> None:
        with pytest.raises(TypeError, match="not JSON serializable"):
            json_dumps({"a": _MyTuple([1, "finite", 2.5])})

    def test_int_min_boundary_ok(self) -> None:
        assert json_loads(json_dumps({"n": -(2**63)})) == {"n": -(2**63)}

    def test_int_below_min_boundary_raises(self) -> None:
        with pytest.raises(ValueError, match="Integer out of range"):
            json_dumps({"n": -(2**63) - 1})

    def test_out_of_range_int_error_message_matches_documented_text(self) -> None:
        with pytest.raises(ValueError) as exc_info:
            json_dumps({"n": 2**64})
        expected = "Integer out of range for JSON serialization (must be within [-2**63, 2**64-1])"
        assert str(exc_info.value) == expected

    def test_int_max_boundary_ok(self) -> None:
        assert json_loads(json_dumps({"n": 2**64 - 1})) == {"n": 2**64 - 1}

    def test_int_above_max_boundary_raises(self) -> None:
        with pytest.raises(ValueError, match="Integer out of range"):
            json_dumps({"n": 2**64})

    def test_out_of_range_int_nested_in_dict_raises(self) -> None:
        with pytest.raises(ValueError, match="Integer out of range"):
            json_dumps({"outer": {"inner": 2**64}})

    def test_out_of_range_int_nested_in_list_raises(self) -> None:
        with pytest.raises(ValueError, match="Integer out of range"):
            json_dumps({"items": [1, 2, [2**64]]})

    def test_out_of_range_int_inside_dict_subclass_raises(self) -> None:
        with pytest.raises(ValueError, match="Integer out of range"):
            json_dumps({"a": _MyDict({"x": 2**64})})

    def test_out_of_range_int_inside_list_subclass_raises(self) -> None:
        with pytest.raises(ValueError, match="Integer out of range"):
            json_dumps({"a": _MyList([-(2**63) - 1])})

    def test_bool_never_range_checked(self) -> None:
        assert json_dumps({"a": True, "b": False}) == '{"a":true,"b":false}'

    def test_int_subclass_in_range_serializes(self) -> None:
        assert json_loads(json_dumps({"c": _Color.RED})) == {"c": 1}

    def test_int_subclass_out_of_range_raises(self) -> None:
        with pytest.raises(ValueError, match="Integer out of range"):
            json_dumps({"n": _MyInt(2**64)})

    def test_float_subclass_finite_serializes(self) -> None:
        assert json_loads(json_dumps({"v": _MyFloat(3.14)})) == {"v": 3.14}

    @pytest.mark.parametrize("value", [float("inf"), float("-inf"), float("nan")])
    def test_float_subclass_non_finite_serializes_as_null(self, value: float) -> None:
        assert json_dumps({"v": _MyFloat(value)}) == '{"v":null}'

    def test_nested_float_subclass_in_dict_subclass_in_list_serializes_as_null(self) -> None:
        data = {"outer": [_MyDict({"inner": _MyFloat(float("inf"))})]}
        assert json_loads(json_dumps(data)) == {"outer": [{"inner": None}]}

    def test_int_subclass_serializes(self) -> None:
        assert json_loads(json_dumps({"v": _MyInt(42)})) == {"v": 42}

    def test_str_subclass_serializes(self) -> None:
        assert json_loads(json_dumps({"v": _MyStr("hi")})) == {"v": "hi"}
