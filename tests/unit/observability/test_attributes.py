"""Tests for eventsource.observability.attributes module."""

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
    """Tests for attribute constant definitions."""

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
    """Tests for specific attribute value correctness."""

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
    """Tests for module exports."""

    def test_all_exports_defined(self):
        for name in attributes.__all__:
            assert hasattr(attributes, name), f"{name} not defined in module"

    def test_imports_from_package(self):
        from eventsource.observability import ATTR_AGGREGATE_ID, ATTR_EVENT_TYPE

        assert ATTR_AGGREGATE_ID == "eventsource.aggregate.id"
        assert ATTR_EVENT_TYPE == "eventsource.event.type"

    def test_attribute_values_are_strings(self):
        for name in attributes.__all__:
            value = getattr(attributes, name)
            assert isinstance(value, str), f"{name} should be a string"

    def test_attribute_values_are_lowercase(self):
        for name in attributes.__all__:
            value = getattr(attributes, name)
            assert value == value.lower(), f"{name} value should be lowercase"

    def test_all_expected_exports_present(self):
        expected = [
            "ATTR_AGGREGATE_ID",
            "ATTR_AGGREGATE_TYPE",
            "ATTR_EVENT_ID",
            "ATTR_EVENT_TYPE",
            "ATTR_EVENT_COUNT",
            "ATTR_VERSION",
            "ATTR_EXPECTED_VERSION",
            "ATTR_TENANT_ID",
            "ATTR_PROJECTION_NAME",
            "ATTR_HANDLER_NAME",
            "ATTR_HANDLER_COUNT",
            "ATTR_POSITION",
            "ATTR_DB_SYSTEM",
            "ATTR_DB_OPERATION",
            "ATTR_MESSAGING_SYSTEM",
            "ATTR_MESSAGING_DESTINATION",
            "ATTR_MESSAGING_OPERATION",
            "ATTR_RETRY_COUNT",
            "ATTR_ERROR_TYPE",
        ]
        for name in expected:
            assert name in attributes.__all__, f"{name} should be in __all__"


class TestPackageLevelImports:
    """Tests for package-level imports from eventsource.observability."""

    def test_all_aggregate_attributes_importable(self):
        from eventsource.observability import ATTR_AGGREGATE_ID, ATTR_AGGREGATE_TYPE

        assert ATTR_AGGREGATE_ID is not None
        assert ATTR_AGGREGATE_TYPE is not None

    def test_all_event_attributes_importable(self):
        from eventsource.observability import ATTR_EVENT_COUNT, ATTR_EVENT_ID, ATTR_EVENT_TYPE

        assert ATTR_EVENT_ID is not None
        assert ATTR_EVENT_TYPE is not None
        assert ATTR_EVENT_COUNT is not None

    def test_all_version_attributes_importable(self):
        from eventsource.observability import ATTR_EXPECTED_VERSION, ATTR_VERSION

        assert ATTR_VERSION is not None
        assert ATTR_EXPECTED_VERSION is not None

    def test_all_tenant_actor_attributes_importable(self):
        assert ATTR_TENANT_ID is not None

    def test_all_component_attributes_importable(self):
        from eventsource.observability import (
            ATTR_HANDLER_COUNT,
            ATTR_HANDLER_NAME,
            ATTR_POSITION,
            ATTR_PROJECTION_NAME,
        )

        assert ATTR_PROJECTION_NAME is not None
        assert ATTR_HANDLER_NAME is not None
        assert ATTR_HANDLER_COUNT is not None
        assert ATTR_POSITION is not None

    def test_all_db_attributes_importable(self):
        from eventsource.observability import ATTR_DB_OPERATION, ATTR_DB_SYSTEM

        assert ATTR_DB_SYSTEM is not None
        assert ATTR_DB_OPERATION is not None

    def test_all_messaging_attributes_importable(self):
        from eventsource.observability import (
            ATTR_MESSAGING_DESTINATION,
            ATTR_MESSAGING_OPERATION,
            ATTR_MESSAGING_SYSTEM,
        )

        assert ATTR_MESSAGING_SYSTEM is not None
        assert ATTR_MESSAGING_DESTINATION is not None
        assert ATTR_MESSAGING_OPERATION is not None

    def test_all_error_retry_attributes_importable(self):
        from eventsource.observability import ATTR_ERROR_TYPE, ATTR_RETRY_COUNT

        assert ATTR_RETRY_COUNT is not None
        assert ATTR_ERROR_TYPE is not None


class TestAttributeUsability:
    """Tests demonstrating practical attribute usage patterns."""

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
