"""
Unit tests for the JSON serialization module.

Tests for:
- EventSourceJSONEncoder class
- json_dumps convenience function
- json_loads convenience function
- Backward compatibility with deprecated imports
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
    """Tests for EventSourceJSONEncoder."""

    def test_encodes_uuid(self):
        """Test encoding UUID to string."""
        test_uuid = uuid4()
        result = json_dumps({"id": test_uuid})
        assert str(test_uuid) in result

    def test_encodes_datetime(self):
        """Test encoding datetime to ISO format string."""
        now = datetime.now(UTC)
        result = json_dumps({"timestamp": now})
        assert now.isoformat() in result

    def test_encodes_datetime_with_microseconds(self):
        """Test encoding datetime with microseconds."""
        test_dt = datetime(2024, 1, 15, 10, 30, 45, 123456, tzinfo=UTC)
        result = json_dumps({"timestamp": test_dt})
        assert "123456" in result

    def test_regular_types_unchanged(self):
        """Test that regular types are encoded normally."""
        data = {"string": "hello", "number": 42, "boolean": True}
        result = json_dumps(data)
        parsed = json_loads(result)
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


class TestDeprecatedImports:
    """Tests for backward compatibility with deprecated import paths."""

    def test_deprecated_import_warns(self):
        """Test that importing from deprecated path shows warning."""
        with pytest.warns(DeprecationWarning, match="eventsource.serialization"):
            from eventsource.repositories._json import json_dumps as _  # noqa: F401

    def test_deprecated_import_works(self):
        """Test that deprecated imports still work correctly."""
        import warnings

        with warnings.catch_warnings():
            warnings.simplefilter("ignore", DeprecationWarning)
            from eventsource.repositories._json import json_dumps

            result = json_dumps({"test": "data"})
            assert "test" in result

    def test_deprecated_encoder_import_warns(self):
        """Test that importing encoder from deprecated path shows warning."""
        with pytest.warns(DeprecationWarning, match="eventsource.serialization"):
            from eventsource.repositories._json import EventSourceJSONEncoder as _  # noqa: F401

    def test_deprecated_loads_import_warns(self):
        """Test that importing json_loads from deprecated path shows warning."""
        with pytest.warns(DeprecationWarning, match="eventsource.serialization"):
            from eventsource.repositories._json import json_loads as _  # noqa: F401

    def test_deprecated_module_dir(self):
        """Test that deprecated module's __dir__ returns expected names."""
        from eventsource.repositories import _json

        available = dir(_json)
        assert "EventSourceJSONEncoder" in available
        assert "json_dumps" in available
        assert "json_loads" in available

    def test_deprecated_module_unknown_attr_raises(self):
        """Test that accessing unknown attr in deprecated module raises AttributeError."""
        with pytest.raises(AttributeError, match="has no attribute"):
            from eventsource.repositories import _json

            _ = _json.unknown_attribute


class TestOrjsonParity:
    """
    Parity tests between the stdlib encoder and orjson.

    These assert byte-identical `json_dumps` output across both encoders,
    because payloads persist in `event_outbox` and `dead_letter_queue` --
    two deployments of the same library version must encode identically
    regardless of whether the `orjson` extra is installed.

    Where orjson and stdlib genuinely cannot agree (dict keys not supported
    by stdlib at all), the divergence is recorded explicitly below rather
    than asserted away.
    """

    def _stdlib_dumps(self, obj, monkeypatch):
        # Route through the real json_dumps() with ORJSON_AVAILABLE forced
        # off, rather than reimplementing the stdlib call inline -- so these
        # parity tests exercise the actual switch logic, not a hand-rolled
        # stand-in that could silently drift from it (as separators and
        # ensure_ascii both did in earlier rounds of this task).
        import eventsource.serialization.json as json_module

        monkeypatch.setattr(json_module, "ORJSON_AVAILABLE", False)
        return json_module.json_dumps(obj)

    def _orjson_dumps(self, obj, monkeypatch):
        import eventsource.serialization.json as json_module

        monkeypatch.setattr(json_module, "ORJSON_AVAILABLE", True)
        return json_module.json_dumps(obj)

    def test_parity_uuid(self, monkeypatch):
        test_uuid = uuid4()
        data = {"id": test_uuid}
        assert self._stdlib_dumps(data, monkeypatch) == self._orjson_dumps(data, monkeypatch)

    def test_parity_aware_datetime(self, monkeypatch):
        dt = datetime(2024, 1, 15, 10, 30, 45, 123456, tzinfo=UTC)
        data = {"t": dt}
        assert self._stdlib_dumps(data, monkeypatch) == self._orjson_dumps(data, monkeypatch)

    def test_parity_naive_datetime(self, monkeypatch):
        dt = datetime(2024, 1, 15, 10, 30, 45, 123456)
        data = {"t": dt}
        assert self._stdlib_dumps(data, monkeypatch) == self._orjson_dumps(data, monkeypatch)

    def test_parity_nested_structure(self, monkeypatch):
        data = {
            "event": {
                "id": uuid4(),
                "occurred_at": datetime(2024, 1, 15, 10, 30, 45, tzinfo=UTC),
                "items": [{"sub_id": uuid4(), "n": 1}, {"sub_id": uuid4(), "n": 2}],
            }
        }
        assert self._stdlib_dumps(data, monkeypatch) == self._orjson_dumps(data, monkeypatch)

    def test_parity_empty_dict(self, monkeypatch):
        assert self._stdlib_dumps({}, monkeypatch) == self._orjson_dumps({}, monkeypatch)

    def test_parity_empty_list(self, monkeypatch):
        assert self._stdlib_dumps([], monkeypatch) == self._orjson_dumps([], monkeypatch)

    def test_parity_none(self, monkeypatch):
        assert self._stdlib_dumps(None, monkeypatch) == self._orjson_dumps(None, monkeypatch)

    def test_parity_int_dict_key(self, monkeypatch):
        # int keys: stdlib stringifies them natively (no encoder.default
        # involvement); orjson requires OPT_NON_STR_KEYS to do the same.
        data = {1: "a", 2: "b"}
        assert self._stdlib_dumps(data, monkeypatch) == self._orjson_dumps(data, monkeypatch)

    def test_parity_str_subclass(self, monkeypatch):
        class MyStr(str):
            pass

        data = {"x": MyStr("hi")}
        assert self._stdlib_dumps(data, monkeypatch) == self._orjson_dumps(data, monkeypatch)

    def test_uuid_dict_key_divergence_is_expected_and_safe(self, monkeypatch):
        """
        Explicit divergence, not a parity gap.

        stdlib's json.dumps (even with EventSourceJSONEncoder) raises
        TypeError for a UUID dict key -- json.JSONEncoder only special-cases
        str/int/float/bool/None keys itself and never calls `default()` for
        keys. orjson, with OPT_NON_STR_KEYS, *can* stringify a UUID key.

        This is safe to leave unmatched: since stdlib could never encode a
        UUID key, no persisted row (written by any build to date) contains
        one. There is nothing to drift between builds for a shape neither
        encoder-before-this-task could produce.
        """
        import json as _json

        from eventsource.serialization.json import EventSourceJSONEncoder

        test_uuid = uuid4()
        with pytest.raises(TypeError):
            _json.dumps({test_uuid: "a"}, cls=EventSourceJSONEncoder)

        # orjson succeeds where stdlib cannot.
        result = self._orjson_dumps({test_uuid: "a"}, monkeypatch)
        assert str(test_uuid) in result

    # Decimal is intentionally excluded: EventSourceJSONEncoder does not
    # support Decimal (json.dumps raises TypeError), and orjson does not
    # either without a `default=` handler -- there is no existing behavior
    # to preserve parity with.

    # -- Content-dimension cases (added in fix round 1) --------------------
    #
    # The type-dimension cases above (UUID, datetime, nested, keys) all
    # happened to use ASCII content, so they missed a real divergence:
    # stdlib's `json.dumps` defaults to `ensure_ascii=True` (escaping
    # non-ASCII as `\uXXXX`), while `orjson.dumps` always emits raw UTF-8
    # with no escaping option. Fixed by adding `ensure_ascii=False` to the
    # stdlib branch of `json_dumps`. These cases cover that content
    # dimension: character sets and value ranges that could plausibly
    # encode differently regardless of Python type.

    def test_parity_latin1_accented_characters(self, monkeypatch):
        data = {"name": "Sébastien Müller Peña"}
        assert self._stdlib_dumps(data, monkeypatch) == self._orjson_dumps(data, monkeypatch)

    def test_parity_non_latin_script_cjk(self, monkeypatch):
        data = {"text": "日本語のテキスト"}
        assert self._stdlib_dumps(data, monkeypatch) == self._orjson_dumps(data, monkeypatch)

    def test_parity_non_latin_script_cyrillic(self, monkeypatch):
        data = {"text": "Привет, мир"}
        assert self._stdlib_dumps(data, monkeypatch) == self._orjson_dumps(data, monkeypatch)

    def test_parity_emoji_outside_bmp(self, monkeypatch):
        # U+1F600 is outside the Basic Multilingual Plane (surrogate-pair
        # territory in UTF-16-based escaping schemes) -- exactly where
        # \uXXXX-escaping implementations most often diverge.
        data = {"reaction": "\U0001f600"}
        assert self._stdlib_dumps(data, monkeypatch) == self._orjson_dumps(data, monkeypatch)

    def test_parity_mixed_ascii_and_non_ascii(self, monkeypatch):
        data = {"msg": "Hello, Sébastien! 日本語 \U0001f600 mixed with ASCII."}
        assert self._stdlib_dumps(data, monkeypatch) == self._orjson_dumps(data, monkeypatch)

    def test_parity_control_characters(self, monkeypatch):
        # \n, \t, and NUL all have well-defined JSON escapes in both
        # encoders (\n, \t, \x00) -- checked and confirmed matching, not
        # assumed safe.
        data = {"text": "line1\nline2\ttab\x00null-byte"}
        assert self._stdlib_dumps(data, monkeypatch) == self._orjson_dumps(data, monkeypatch)

    def test_parity_large_integer_beyond_53_bit_precision(self, monkeypatch):
        # Python ints are arbitrary precision; JSON numbers have no
        # official limit either. Checked and confirmed both encoders emit
        # the exact decimal digits with no float coercion / precision loss.
        data = {"n": 12345678901234567890}
        assert self._stdlib_dumps(data, monkeypatch) == self._orjson_dumps(data, monkeypatch)

    def test_parity_high_precision_float(self, monkeypatch):
        data = {"v": 1.123456789012345}
        assert self._stdlib_dumps(data, monkeypatch) == self._orjson_dumps(data, monkeypatch)

    def test_parity_negative_zero_float(self, monkeypatch):
        data = {"v": -0.0}
        assert self._stdlib_dumps(data, monkeypatch) == self._orjson_dumps(data, monkeypatch)

    def test_parity_empty_and_whitespace_strings(self, monkeypatch):
        data = {"empty": "", "spaces": "   ", "tabs_newlines": "\t\n"}
        assert self._stdlib_dumps(data, monkeypatch) == self._orjson_dumps(data, monkeypatch)

    # -- Non-finite floats (added in fix round 2) ---------------------------
    #
    # Round 1 found a genuine, NOT-safe divergence here and reported it
    # rather than resolving it: stdlib's json.dumps (allow_nan=True, the
    # default) emitted the non-standard bare tokens Infinity/-Infinity/NaN,
    # which most JSON parsers -- and PostgreSQL jsonb -- reject outright;
    # orjson has no option to preserve those tokens and always silently
    # substituted JSON `null` instead. Decision (round 2): reject non-finite
    # floats at serialization time in BOTH branches, with the same
    # exception type and message, rather than let either branch produce a
    # value (Infinity/NaN or null) that a two-build deployment could
    # disagree about. The divergence is gone; these tests assert the
    # rejection, not the old divergence.

    @pytest.mark.parametrize("value", [float("inf"), float("-inf"), float("nan")])
    def test_non_finite_float_raises_from_stdlib_fallback(self, value, monkeypatch):
        import eventsource.serialization.json as json_module

        monkeypatch.setattr(json_module, "ORJSON_AVAILABLE", False)
        with pytest.raises(ValueError, match="not JSON compliant"):
            json_module.json_dumps({"v": value})

    @pytest.mark.parametrize("value", [float("inf"), float("-inf"), float("nan")])
    def test_non_finite_float_raises_from_orjson_path(self, value, monkeypatch):
        import eventsource.serialization.json as json_module

        monkeypatch.setattr(json_module, "ORJSON_AVAILABLE", True)
        with pytest.raises(ValueError, match="not JSON compliant"):
            json_module.json_dumps({"v": value})

    @pytest.mark.parametrize("orjson_available", [True, False])
    def test_non_finite_float_nested_in_list_raises(self, orjson_available, monkeypatch):
        import eventsource.serialization.json as json_module

        monkeypatch.setattr(json_module, "ORJSON_AVAILABLE", orjson_available)
        data = {"items": [1.0, 2.0, {"nested": [float("nan")]}]}
        with pytest.raises(ValueError, match="not JSON compliant"):
            json_module.json_dumps(data)

    @pytest.mark.parametrize("orjson_available", [True, False])
    def test_non_finite_float_nested_in_dict_raises(self, orjson_available, monkeypatch):
        import eventsource.serialization.json as json_module

        monkeypatch.setattr(json_module, "ORJSON_AVAILABLE", orjson_available)
        data = {"outer": {"inner": {"deep": float("inf")}}}
        with pytest.raises(ValueError, match="not JSON compliant"):
            json_module.json_dumps(data)

    def test_both_branches_raise_the_same_exception_type_and_message(self, monkeypatch):
        import eventsource.serialization.json as json_module

        data = {"v": float("inf")}

        monkeypatch.setattr(json_module, "ORJSON_AVAILABLE", False)
        with pytest.raises(ValueError) as stdlib_exc:
            json_module.json_dumps(data)

        monkeypatch.setattr(json_module, "ORJSON_AVAILABLE", True)
        with pytest.raises(ValueError) as orjson_exc:
            json_module.json_dumps(data)

        assert type(stdlib_exc.value) is type(orjson_exc.value) is ValueError
        assert str(stdlib_exc.value) == str(orjson_exc.value)

    def test_finite_floats_still_parity_after_non_finite_rejection(self, monkeypatch):
        # Guards against a rejection implementation that's overly broad
        # (e.g. rejecting all floats, or misclassifying -0.0 as special).
        data = {
            "zero": 0.0,
            "neg_zero": -0.0,
            "tiny": 5e-300,
            "huge": 1.7e300,
            "precise": 1.123456789012345,
            "negative": -42.5,
        }
        assert self._stdlib_dumps(data, monkeypatch) == self._orjson_dumps(data, monkeypatch)


class TestOrjsonFallback:
    """
    Prove the stdlib fallback path actually runs, by forcing
    `ORJSON_AVAILABLE = False` even in an environment where orjson is
    installed. A fallback branch that is never exercised is not a fallback.
    """

    def test_json_dumps_falls_back_to_stdlib(self, monkeypatch):
        import eventsource.serialization.json as json_module

        monkeypatch.setattr(json_module, "ORJSON_AVAILABLE", False)

        test_uuid = uuid4()
        dt = datetime(2024, 1, 15, 10, 30, 45, 123456, tzinfo=UTC)
        data = {"id": test_uuid, "t": dt}

        result = json_module.json_dumps(data)

        assert result == json.dumps(
            data, cls=json_module.EventSourceJSONEncoder, separators=(",", ":")
        )
        parsed = json.loads(result)
        assert parsed["id"] == str(test_uuid)
        assert parsed["t"] == dt.isoformat()

    def test_json_loads_falls_back_to_stdlib(self, monkeypatch):
        import eventsource.serialization.json as json_module

        monkeypatch.setattr(json_module, "ORJSON_AVAILABLE", False)

        result = json_module.json_loads('{"a": 1, "b": [1, 2, 3]}')

        assert result == {"a": 1, "b": [1, 2, 3]}

    def test_fallback_output_matches_orjson_output(self, monkeypatch):
        """
        With ORJSON_AVAILABLE forced False, json_dumps must still produce
        output byte-identical to the orjson path -- this is the actual
        regression this task guards against: format drift between builds.
        """
        import eventsource.serialization.json as json_module

        data = {
            "id": uuid4(),
            "t": datetime(2024, 1, 15, 10, 30, 45, 123456, tzinfo=UTC),
            "nested": {"items": [1, 2, {"x": None}]},
        }

        monkeypatch.setattr(json_module, "ORJSON_AVAILABLE", True)
        with_orjson = json_module.json_dumps(data)

        monkeypatch.setattr(json_module, "ORJSON_AVAILABLE", False)
        with_stdlib_fallback = json_module.json_dumps(data)

        assert with_orjson == with_stdlib_fallback


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

    def test_encoder_still_exported_from_repositories(self):
        """Test that EventSourceJSONEncoder is still exported from repositories."""
        from eventsource.repositories import EventSourceJSONEncoder

        # Verify encoder is a class
        assert isinstance(EventSourceJSONEncoder, type)

        # Verify it works
        result = json.dumps({"id": uuid4()}, cls=EventSourceJSONEncoder)
        assert isinstance(result, str)

    def test_encoder_exported_from_top_level(self):
        """Test that EventSourceJSONEncoder is exported from eventsource package."""
        from eventsource import EventSourceJSONEncoder

        # Verify encoder is a class
        assert isinstance(EventSourceJSONEncoder, type)

        # Verify it works
        result = json.dumps({"id": uuid4()}, cls=EventSourceJSONEncoder)
        assert isinstance(result, str)
