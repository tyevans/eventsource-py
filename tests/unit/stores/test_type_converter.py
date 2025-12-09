"""Unit tests for TypeConverter and DefaultTypeConverter."""

from datetime import datetime
from uuid import UUID

from eventsource.stores._type_converter import (
    DEFAULT_STRING_ID_FIELDS,
    DEFAULT_UUID_FIELDS,
    DefaultTypeConverter,
    TypeConverter,
)


class TestTypeConverterProtocol:
    """Tests for the TypeConverter protocol."""

    def test_protocol_is_runtime_checkable(self) -> None:
        """TypeConverter protocol should be runtime checkable."""

        class CustomConverter:
            def convert_types(self, data):
                return data

            def is_uuid_field(self, key):
                return False

            def is_datetime_field(self, key):
                return False

        assert isinstance(CustomConverter(), TypeConverter)

    def test_protocol_rejects_incomplete_implementation(self) -> None:
        """Protocol should reject classes missing methods."""

        class IncompleteConverter:
            def convert_types(self, data):
                return data

        assert not isinstance(IncompleteConverter(), TypeConverter)


class TestDefaultTypeConverterInit:
    """Tests for DefaultTypeConverter initialization."""

    def test_default_initialization(self) -> None:
        """Default init should use built-in field sets."""
        converter = DefaultTypeConverter()
        assert converter._uuid_fields == DEFAULT_UUID_FIELDS
        assert converter._string_id_fields == DEFAULT_STRING_ID_FIELDS
        assert converter._auto_detect_uuid is True

    def test_custom_uuid_fields_merged_with_defaults(self) -> None:
        """Custom UUID fields should merge with defaults."""
        converter = DefaultTypeConverter(uuid_fields={"custom_id"})
        assert "custom_id" in converter._uuid_fields
        assert "event_id" in converter._uuid_fields  # Default still present

    def test_custom_string_id_fields_merged_with_defaults(self) -> None:
        """Custom string ID fields should merge with defaults."""
        converter = DefaultTypeConverter(string_id_fields={"stripe_id"})
        assert "stripe_id" in converter._string_id_fields
        assert "actor_id" in converter._string_id_fields  # Default still present

    def test_auto_detect_disabled(self) -> None:
        """Auto-detection can be disabled."""
        converter = DefaultTypeConverter(auto_detect_uuid=False)
        assert converter._auto_detect_uuid is False

    def test_use_defaults_false(self) -> None:
        """use_defaults=False should not include built-in sets."""
        converter = DefaultTypeConverter(
            uuid_fields={"only_this"},
            use_defaults=False,
        )
        assert converter._uuid_fields == frozenset({"only_this"})
        assert converter._string_id_fields == frozenset()

    def test_strict_mode_factory(self) -> None:
        """strict() factory should create minimal converter."""
        converter = DefaultTypeConverter.strict({"event_id", "aggregate_id"})
        assert converter._uuid_fields == frozenset({"event_id", "aggregate_id"})
        assert converter._string_id_fields == frozenset()
        assert converter._auto_detect_uuid is False


class TestIsUuidField:
    """Tests for is_uuid_field method."""

    def test_explicit_uuid_field_returns_true(self) -> None:
        """Fields in uuid_fields should return True."""
        converter = DefaultTypeConverter()
        assert converter.is_uuid_field("event_id") is True
        assert converter.is_uuid_field("aggregate_id") is True
        assert converter.is_uuid_field("tenant_id") is True

    def test_string_id_field_returns_false(self) -> None:
        """Fields in string_id_fields should return False."""
        converter = DefaultTypeConverter()
        assert converter.is_uuid_field("actor_id") is False
        assert converter.is_uuid_field("issuer_id") is False

    def test_auto_detect_id_suffix(self) -> None:
        """Fields ending in _id should be auto-detected as UUID."""
        converter = DefaultTypeConverter()
        assert converter.is_uuid_field("custom_reference_id") is True
        assert converter.is_uuid_field("parent_id") is True

    def test_auto_detect_disabled_no_suffix_detection(self) -> None:
        """With auto_detect=False, _id suffix should not match."""
        converter = DefaultTypeConverter(auto_detect_uuid=False)
        assert converter.is_uuid_field("custom_id") is False
        # But explicit fields still work
        assert converter.is_uuid_field("event_id") is True

    def test_non_id_field_returns_false(self) -> None:
        """Fields not ending in _id should return False."""
        converter = DefaultTypeConverter()
        assert converter.is_uuid_field("name") is False
        assert converter.is_uuid_field("email") is False
        assert converter.is_uuid_field("id_prefix") is False

    def test_priority_explicit_over_exclusion(self) -> None:
        """Explicit UUID fields take priority over exclusions."""
        converter = DefaultTypeConverter(
            uuid_fields={"actor_id"},  # Normally excluded
        )
        # actor_id is both in uuid_fields AND string_id_fields
        # uuid_fields should win
        assert converter.is_uuid_field("actor_id") is True


class TestIsDatetimeField:
    """Tests for is_datetime_field method."""

    def test_occurred_at_returns_true(self) -> None:
        """occurred_at should always return True."""
        converter = DefaultTypeConverter()
        assert converter.is_datetime_field("occurred_at") is True

    def test_at_suffix_returns_true(self) -> None:
        """Fields ending in _at should return True."""
        converter = DefaultTypeConverter()
        assert converter.is_datetime_field("created_at") is True
        assert converter.is_datetime_field("updated_at") is True
        assert converter.is_datetime_field("deleted_at") is True

    def test_non_at_field_returns_false(self) -> None:
        """Fields not ending in _at should return False."""
        converter = DefaultTypeConverter()
        assert converter.is_datetime_field("timestamp") is False
        assert converter.is_datetime_field("date") is False
        assert converter.is_datetime_field("at_start") is False


class TestConvertTypes:
    """Tests for convert_types method."""

    def test_convert_uuid_string(self) -> None:
        """UUID strings should be converted to UUID objects."""
        converter = DefaultTypeConverter()
        data = {"event_id": "550e8400-e29b-41d4-a716-446655440000"}
        result = converter.convert_types(data)
        assert isinstance(result["event_id"], UUID)
        assert str(result["event_id"]) == "550e8400-e29b-41d4-a716-446655440000"

    def test_convert_datetime_string_with_z(self) -> None:
        """Datetime strings with Z suffix should be converted."""
        converter = DefaultTypeConverter()
        data = {"occurred_at": "2024-01-01T12:00:00Z"}
        result = converter.convert_types(data)
        assert isinstance(result["occurred_at"], datetime)
        assert result["occurred_at"].tzinfo is not None

    def test_convert_datetime_string_with_offset(self) -> None:
        """Datetime strings with offset should be converted."""
        converter = DefaultTypeConverter()
        data = {"created_at": "2024-01-01T12:00:00+00:00"}
        result = converter.convert_types(data)
        assert isinstance(result["created_at"], datetime)

    def test_preserve_string_id_fields(self) -> None:
        """String ID fields should remain as strings."""
        converter = DefaultTypeConverter()
        data = {"actor_id": "user@example.com"}
        result = converter.convert_types(data)
        assert isinstance(result["actor_id"], str)
        assert result["actor_id"] == "user@example.com"

    def test_preserve_non_matching_fields(self) -> None:
        """Fields not matching patterns should be preserved."""
        converter = DefaultTypeConverter()
        data = {"name": "John", "count": 42, "active": True}
        result = converter.convert_types(data)
        assert result["name"] == "John"
        assert result["count"] == 42
        assert result["active"] is True

    def test_nested_dict_conversion(self) -> None:
        """Nested dictionaries should be recursively converted."""
        converter = DefaultTypeConverter()
        data = {
            "metadata": {
                "user_id": "550e8400-e29b-41d4-a716-446655440000",
                "created_at": "2024-01-01T12:00:00Z",
            }
        }
        result = converter.convert_types(data)
        assert isinstance(result["metadata"]["user_id"], UUID)
        assert isinstance(result["metadata"]["created_at"], datetime)

    def test_deeply_nested_conversion(self) -> None:
        """Three levels of nesting should be converted."""
        converter = DefaultTypeConverter()
        data = {
            "level1": {
                "level2": {
                    "level3": {
                        "event_id": "550e8400-e29b-41d4-a716-446655440000",
                    }
                }
            }
        }
        result = converter.convert_types(data)
        assert isinstance(result["level1"]["level2"]["level3"]["event_id"], UUID)

    def test_list_conversion(self) -> None:
        """Lists should have their elements converted."""
        converter = DefaultTypeConverter()
        data = {
            "items": [
                {"item_id": "550e8400-e29b-41d4-a716-446655440000"},
                {"item_id": "660e8400-e29b-41d4-a716-446655440001"},
            ]
        }
        result = converter.convert_types(data)
        assert isinstance(result["items"][0]["item_id"], UUID)
        assert isinstance(result["items"][1]["item_id"], UUID)

    def test_top_level_list(self) -> None:
        """Top-level lists should be converted."""
        converter = DefaultTypeConverter()
        data = [
            {"event_id": "550e8400-e29b-41d4-a716-446655440000"},
            {"event_id": "660e8400-e29b-41d4-a716-446655440001"},
        ]
        result = converter.convert_types(data)
        assert isinstance(result[0]["event_id"], UUID)
        assert isinstance(result[1]["event_id"], UUID)

    def test_invalid_uuid_preserved(self) -> None:
        """Invalid UUID strings should be preserved as-is."""
        converter = DefaultTypeConverter()
        data = {"event_id": "not-a-valid-uuid"}
        result = converter.convert_types(data)
        assert result["event_id"] == "not-a-valid-uuid"
        assert isinstance(result["event_id"], str)

    def test_invalid_datetime_preserved(self) -> None:
        """Invalid datetime strings should be preserved as-is."""
        converter = DefaultTypeConverter()
        data = {"created_at": "not-a-valid-datetime"}
        result = converter.convert_types(data)
        assert result["created_at"] == "not-a-valid-datetime"
        assert isinstance(result["created_at"], str)

    def test_none_values_preserved(self) -> None:
        """None values should be preserved."""
        converter = DefaultTypeConverter()
        data = {"event_id": None, "occurred_at": None}
        result = converter.convert_types(data)
        assert result["event_id"] is None
        assert result["occurred_at"] is None

    def test_empty_dict(self) -> None:
        """Empty dict should return empty dict."""
        converter = DefaultTypeConverter()
        assert converter.convert_types({}) == {}

    def test_empty_list(self) -> None:
        """Empty list should return empty list."""
        converter = DefaultTypeConverter()
        assert converter.convert_types([]) == []

    def test_scalar_passthrough(self) -> None:
        """Scalar values should pass through unchanged."""
        converter = DefaultTypeConverter()
        assert converter.convert_types("string") == "string"
        assert converter.convert_types(42) == 42
        assert converter.convert_types(None) is None

    def test_original_data_not_modified(self) -> None:
        """Original data should not be modified."""
        converter = DefaultTypeConverter()
        original = {"event_id": "550e8400-e29b-41d4-a716-446655440000"}
        original_copy = dict(original)
        converter.convert_types(original)
        assert original == original_copy

    def test_mixed_valid_invalid_uuids(self) -> None:
        """Mix of valid and invalid UUIDs should be handled."""
        converter = DefaultTypeConverter()
        data = {
            "event_id": "550e8400-e29b-41d4-a716-446655440000",
            "aggregate_id": "invalid",
        }
        result = converter.convert_types(data)
        assert isinstance(result["event_id"], UUID)
        assert result["aggregate_id"] == "invalid"


class TestDefaultFieldSets:
    """Tests for default field set constants."""

    def test_default_uuid_fields_contains_expected(self) -> None:
        """DEFAULT_UUID_FIELDS should contain standard event fields."""
        expected = {
            "event_id",
            "aggregate_id",
            "tenant_id",
            "correlation_id",
            "causation_id",
        }
        assert expected.issubset(DEFAULT_UUID_FIELDS)

    def test_default_string_id_fields_contains_expected(self) -> None:
        """DEFAULT_STRING_ID_FIELDS should contain actor fields."""
        expected = {"actor_id", "issuer_id", "recipient_id"}
        assert expected.issubset(DEFAULT_STRING_ID_FIELDS)

    def test_default_sets_are_frozen(self) -> None:
        """Default sets should be frozen (immutable)."""
        assert isinstance(DEFAULT_UUID_FIELDS, frozenset)
        assert isinstance(DEFAULT_STRING_ID_FIELDS, frozenset)
