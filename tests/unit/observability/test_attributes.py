"""Tests for eventsource.observability.attributes module."""

import eventsource.observability as obs_pkg
from eventsource.observability import attributes
from eventsource.observability.attributes import (
    ATTR_AGGREGATE_ID,
    ATTR_AGGREGATE_TYPE,
    ATTR_DB_OPERATION,
    ATTR_DB_SYSTEM,
    ATTR_ERROR_TYPE,
    ATTR_EVENT_COUNT,
    ATTR_EVENT_ID,
    ATTR_EVENT_TYPE,
    ATTR_EXPECTED_VERSION,
    ATTR_HANDLER_COUNT,
    ATTR_HANDLER_NAME,
    ATTR_MESSAGING_DESTINATION,
    ATTR_MESSAGING_OPERATION,
    ATTR_MESSAGING_SYSTEM,
    ATTR_POSITION,
    ATTR_PROJECTION_NAME,
    ATTR_RETRY_COUNT,
    ATTR_TENANT_ID,
    ATTR_VERSION,
)


class TestAttributeConstants:
    def test_aggregate_attributes_have_eventsource_prefix(self):
        assert ATTR_AGGREGATE_ID.startswith("eventsource.")
        assert ATTR_AGGREGATE_TYPE.startswith("eventsource.")

    def test_event_attributes_have_eventsource_prefix(self):
        assert ATTR_EVENT_ID.startswith("eventsource.")
        assert ATTR_EVENT_TYPE.startswith("eventsource.")
        assert ATTR_EVENT_COUNT.startswith("eventsource.")

    def test_version_attributes_have_eventsource_prefix(self):
        assert ATTR_VERSION.startswith("eventsource.")
        assert ATTR_EXPECTED_VERSION.startswith("eventsource.")

    def test_tenant_attributes_have_eventsource_prefix(self):
        assert ATTR_TENANT_ID.startswith("eventsource.")

    def test_component_attributes_have_eventsource_prefix(self):
        assert ATTR_PROJECTION_NAME.startswith("eventsource.")
        assert ATTR_HANDLER_NAME.startswith("eventsource.")
        assert ATTR_HANDLER_COUNT.startswith("eventsource.")
        assert ATTR_POSITION.startswith("eventsource.")

    def test_error_retry_attributes_have_eventsource_prefix(self):
        assert ATTR_RETRY_COUNT.startswith("eventsource.")
        assert ATTR_ERROR_TYPE.startswith("eventsource.")

    def test_db_attributes_follow_otel_conventions(self):
        assert ATTR_DB_SYSTEM == "db.system"
        assert ATTR_DB_OPERATION == "db.operation"

    def test_messaging_attributes_follow_otel_conventions(self):
        assert ATTR_MESSAGING_SYSTEM == "messaging.system"
        assert ATTR_MESSAGING_DESTINATION == "messaging.destination"
        assert ATTR_MESSAGING_OPERATION == "messaging.operation"


class TestAttributeValues:
    def test_aggregate_id_value(self):
        assert ATTR_AGGREGATE_ID == "eventsource.aggregate.id"

    def test_aggregate_type_value(self):
        assert ATTR_AGGREGATE_TYPE == "eventsource.aggregate.type"

    def test_event_id_value(self):
        assert ATTR_EVENT_ID == "eventsource.event.id"

    def test_event_type_value(self):
        assert ATTR_EVENT_TYPE == "eventsource.event.type"

    def test_event_count_value(self):
        assert ATTR_EVENT_COUNT == "eventsource.event.count"

    def test_version_value(self):
        assert ATTR_VERSION == "eventsource.version"

    def test_expected_version_value(self):
        assert ATTR_EXPECTED_VERSION == "eventsource.expected_version"

    def test_tenant_id_value(self):
        assert ATTR_TENANT_ID == "eventsource.tenant.id"

    def test_projection_name_value(self):
        assert ATTR_PROJECTION_NAME == "eventsource.projection.name"

    def test_handler_name_value(self):
        assert ATTR_HANDLER_NAME == "eventsource.handler.name"

    def test_handler_count_value(self):
        assert ATTR_HANDLER_COUNT == "eventsource.handler.count"

    def test_position_value(self):
        assert ATTR_POSITION == "eventsource.position"

    def test_retry_count_value(self):
        assert ATTR_RETRY_COUNT == "eventsource.retry.count"

    def test_error_type_value(self):
        assert ATTR_ERROR_TYPE == "eventsource.error.type"


class TestAttributeExports:
    def test_all_exports_defined(self):
        for name in attributes.__all__:
            assert hasattr(attributes, name), f"{name} not defined in module"

    def test_imports_from_package(self):
        assert obs_pkg.ATTR_AGGREGATE_ID == "eventsource.aggregate.id"
        assert obs_pkg.ATTR_EVENT_TYPE == "eventsource.event.type"

    def test_attribute_values_are_strings(self):
        for name in attributes.__all__:
            value = getattr(attributes, name)
            assert isinstance(value, str), f"{name} should be a string"

    def test_attribute_values_are_lowercase(self):
        for name in attributes.__all__:
            value = getattr(attributes, name)
            assert value == value.lower(), f"{name} value should be lowercase"

    def test_all_expected_exports_present(self):
        expected = [f"ATTR_{s}" for s in (
            "AGGREGATE_ID", "AGGREGATE_TYPE", "EVENT_ID", "EVENT_TYPE", "EVENT_COUNT",
            "VERSION", "EXPECTED_VERSION", "TENANT_ID", "PROJECTION_NAME", "HANDLER_NAME",
            "HANDLER_COUNT", "POSITION", "DB_SYSTEM", "DB_OPERATION", "MESSAGING_SYSTEM",
            "MESSAGING_DESTINATION", "MESSAGING_OPERATION", "RETRY_COUNT", "ERROR_TYPE",
        )]  # fmt: skip
        for name in expected:
            assert name in attributes.__all__, f"{name} should be in __all__"


class TestPackageLevelImports:
    def test_all_aggregate_attributes_importable(self):
        assert obs_pkg.ATTR_AGGREGATE_ID is not None and obs_pkg.ATTR_AGGREGATE_TYPE is not None

    def test_all_event_attributes_importable(self):
        assert None not in (
            obs_pkg.ATTR_EVENT_ID,
            obs_pkg.ATTR_EVENT_TYPE,
            obs_pkg.ATTR_EVENT_COUNT,
        )

    def test_all_version_attributes_importable(self):
        assert obs_pkg.ATTR_VERSION is not None and obs_pkg.ATTR_EXPECTED_VERSION is not None

    def test_all_tenant_actor_attributes_importable(self):
        assert obs_pkg.ATTR_TENANT_ID is not None

    def test_all_component_attributes_importable(self):
        assert None not in (
            obs_pkg.ATTR_PROJECTION_NAME,
            obs_pkg.ATTR_HANDLER_NAME,
            obs_pkg.ATTR_HANDLER_COUNT,
            obs_pkg.ATTR_POSITION,
        )

    def test_all_db_attributes_importable(self):
        assert obs_pkg.ATTR_DB_SYSTEM is not None and obs_pkg.ATTR_DB_OPERATION is not None

    def test_all_messaging_attributes_importable(self):
        assert None not in (
            obs_pkg.ATTR_MESSAGING_SYSTEM,
            obs_pkg.ATTR_MESSAGING_DESTINATION,
            obs_pkg.ATTR_MESSAGING_OPERATION,
        )

    def test_all_error_retry_attributes_importable(self):
        assert obs_pkg.ATTR_RETRY_COUNT is not None and obs_pkg.ATTR_ERROR_TYPE is not None


class TestAttributeUsability:
    def test_attributes_usable_as_dict_keys(self):
        span_attributes = {
            ATTR_AGGREGATE_ID: "123e4567-e89b-12d3-a456-426614174000",
            ATTR_EVENT_TYPE: "UserCreated",
            ATTR_VERSION: 1,
        }
        assert span_attributes[ATTR_AGGREGATE_ID] == "123e4567-e89b-12d3-a456-426614174000"
        assert span_attributes[ATTR_EVENT_TYPE] == "UserCreated"
        assert span_attributes[ATTR_VERSION] == 1

    def test_attributes_unique(self):
        all_values = [getattr(attributes, name) for name in attributes.__all__]
        assert len(all_values) == len(set(all_values)), "Attribute values should be unique"

    def test_no_trailing_dots_in_values(self):
        for name in attributes.__all__:
            value = getattr(attributes, name)
            assert not value.endswith("."), f"{name} should not end with a dot"

    def test_no_leading_dots_in_values(self):
        for name in attributes.__all__:
            value = getattr(attributes, name)
            assert not value.startswith("."), f"{name} should not start with a dot"
