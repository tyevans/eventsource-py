"""Tests for eventsource.observability.attributes module."""

from eventsource.observability import attributes
from eventsource.observability.attributes import (
    ATTR_ACTOR_ID,
    ATTR_AGGREGATE_ID,
    ATTR_AGGREGATE_TYPE,
    ATTR_DB_NAME,
    ATTR_DB_OPERATION,
    ATTR_DB_SYSTEM,
    ATTR_ERROR_TYPE,
    ATTR_EVENT_COUNT,
    ATTR_EVENT_ID,
    ATTR_EVENT_TYPE,
    ATTR_EXPECTED_VERSION,
    ATTR_FROM_VERSION,
    ATTR_HANDLER_COUNT,
    ATTR_HANDLER_NAME,
    ATTR_MESSAGING_DESTINATION,
    ATTR_MESSAGING_OPERATION,
    ATTR_MESSAGING_SYSTEM,
    ATTR_POSITION,
    ATTR_PROJECTION_NAME,
    ATTR_RETRY_COUNT,
    ATTR_STREAM_ID,
    ATTR_TENANT_ID,
    ATTR_VERSION,
)


class TestAttributeConstants:
    """Tests for attribute constant definitions."""

    def test_aggregate_attributes_have_eventsource_prefix(self):
        """Aggregate attributes should use eventsource prefix."""
        assert ATTR_AGGREGATE_ID.startswith("eventsource.")
        assert ATTR_AGGREGATE_TYPE.startswith("eventsource.")

    def test_event_attributes_have_eventsource_prefix(self):
        """Event attributes should use eventsource prefix."""
        assert ATTR_EVENT_ID.startswith("eventsource.")
        assert ATTR_EVENT_TYPE.startswith("eventsource.")
        assert ATTR_EVENT_COUNT.startswith("eventsource.")

    def test_version_attributes_have_eventsource_prefix(self):
        """Version attributes should use eventsource prefix."""
        assert ATTR_VERSION.startswith("eventsource.")
        assert ATTR_EXPECTED_VERSION.startswith("eventsource.")
        assert ATTR_FROM_VERSION.startswith("eventsource.")

    def test_tenant_actor_attributes_have_eventsource_prefix(self):
        """Tenant and actor attributes should use eventsource prefix."""
        assert ATTR_TENANT_ID.startswith("eventsource.")
        assert ATTR_ACTOR_ID.startswith("eventsource.")

    def test_component_attributes_have_eventsource_prefix(self):
        """Component-specific attributes should use eventsource prefix."""
        assert ATTR_PROJECTION_NAME.startswith("eventsource.")
        assert ATTR_HANDLER_NAME.startswith("eventsource.")
        assert ATTR_HANDLER_COUNT.startswith("eventsource.")
        assert ATTR_STREAM_ID.startswith("eventsource.")
        assert ATTR_POSITION.startswith("eventsource.")

    def test_error_retry_attributes_have_eventsource_prefix(self):
        """Error and retry attributes should use eventsource prefix."""
        assert ATTR_RETRY_COUNT.startswith("eventsource.")
        assert ATTR_ERROR_TYPE.startswith("eventsource.")

    def test_db_attributes_follow_otel_conventions(self):
        """Database attributes should follow OTEL semantic conventions."""
        assert ATTR_DB_SYSTEM == "db.system"
        assert ATTR_DB_NAME == "db.name"
        assert ATTR_DB_OPERATION == "db.operation"

    def test_messaging_attributes_follow_otel_conventions(self):
        """Messaging attributes should follow OTEL semantic conventions."""
        assert ATTR_MESSAGING_SYSTEM == "messaging.system"
        assert ATTR_MESSAGING_DESTINATION == "messaging.destination"
        assert ATTR_MESSAGING_OPERATION == "messaging.operation"


class TestAttributeValues:
    """Tests for specific attribute value correctness."""

    def test_aggregate_id_value(self):
        """ATTR_AGGREGATE_ID has correct value."""
        assert ATTR_AGGREGATE_ID == "eventsource.aggregate.id"

    def test_aggregate_type_value(self):
        """ATTR_AGGREGATE_TYPE has correct value."""
        assert ATTR_AGGREGATE_TYPE == "eventsource.aggregate.type"

    def test_event_id_value(self):
        """ATTR_EVENT_ID has correct value."""
        assert ATTR_EVENT_ID == "eventsource.event.id"

    def test_event_type_value(self):
        """ATTR_EVENT_TYPE has correct value."""
        assert ATTR_EVENT_TYPE == "eventsource.event.type"

    def test_event_count_value(self):
        """ATTR_EVENT_COUNT has correct value."""
        assert ATTR_EVENT_COUNT == "eventsource.event.count"

    def test_version_value(self):
        """ATTR_VERSION has correct value."""
        assert ATTR_VERSION == "eventsource.version"

    def test_expected_version_value(self):
        """ATTR_EXPECTED_VERSION has correct value."""
        assert ATTR_EXPECTED_VERSION == "eventsource.expected_version"

    def test_from_version_value(self):
        """ATTR_FROM_VERSION has correct value."""
        assert ATTR_FROM_VERSION == "eventsource.from_version"

    def test_tenant_id_value(self):
        """ATTR_TENANT_ID has correct value."""
        assert ATTR_TENANT_ID == "eventsource.tenant.id"

    def test_actor_id_value(self):
        """ATTR_ACTOR_ID has correct value."""
        assert ATTR_ACTOR_ID == "eventsource.actor.id"

    def test_projection_name_value(self):
        """ATTR_PROJECTION_NAME has correct value."""
        assert ATTR_PROJECTION_NAME == "eventsource.projection.name"

    def test_handler_name_value(self):
        """ATTR_HANDLER_NAME has correct value."""
        assert ATTR_HANDLER_NAME == "eventsource.handler.name"

    def test_handler_count_value(self):
        """ATTR_HANDLER_COUNT has correct value."""
        assert ATTR_HANDLER_COUNT == "eventsource.handler.count"

    def test_stream_id_value(self):
        """ATTR_STREAM_ID has correct value."""
        assert ATTR_STREAM_ID == "eventsource.stream.id"

    def test_position_value(self):
        """ATTR_POSITION has correct value."""
        assert ATTR_POSITION == "eventsource.position"

    def test_retry_count_value(self):
        """ATTR_RETRY_COUNT has correct value."""
        assert ATTR_RETRY_COUNT == "eventsource.retry.count"

    def test_error_type_value(self):
        """ATTR_ERROR_TYPE has correct value."""
        assert ATTR_ERROR_TYPE == "eventsource.error.type"


class TestAttributeExports:
    """Tests for module exports."""

    def test_all_exports_defined(self):
        """All __all__ items should be defined in module."""
        for name in attributes.__all__:
            assert hasattr(attributes, name), f"{name} not defined in module"

    def test_imports_from_package(self):
        """Attributes should be importable from observability package."""
        from eventsource.observability import (
            ATTR_AGGREGATE_ID,
            ATTR_EVENT_TYPE,
        )

        assert ATTR_AGGREGATE_ID == "eventsource.aggregate.id"
        assert ATTR_EVENT_TYPE == "eventsource.event.type"

    def test_attribute_values_are_strings(self):
        """All attribute constants should be strings."""
        for name in attributes.__all__:
            value = getattr(attributes, name)
            assert isinstance(value, str), f"{name} should be a string"

    def test_attribute_values_are_lowercase(self):
        """All attribute values should be lowercase (OTEL convention)."""
        for name in attributes.__all__:
            value = getattr(attributes, name)
            assert value == value.lower(), f"{name} value should be lowercase"

    def test_all_expected_exports_present(self):
        """All expected attribute constants are in __all__."""
        expected = [
            "ATTR_AGGREGATE_ID",
            "ATTR_AGGREGATE_TYPE",
            "ATTR_EVENT_ID",
            "ATTR_EVENT_TYPE",
            "ATTR_EVENT_COUNT",
            "ATTR_VERSION",
            "ATTR_EXPECTED_VERSION",
            "ATTR_FROM_VERSION",
            "ATTR_TENANT_ID",
            "ATTR_ACTOR_ID",
            "ATTR_PROJECTION_NAME",
            "ATTR_HANDLER_NAME",
            "ATTR_HANDLER_COUNT",
            "ATTR_STREAM_ID",
            "ATTR_POSITION",
            "ATTR_DB_SYSTEM",
            "ATTR_DB_NAME",
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
        """All aggregate attributes are importable from package."""
        from eventsource.observability import (
            ATTR_AGGREGATE_ID,
            ATTR_AGGREGATE_TYPE,
        )

        assert ATTR_AGGREGATE_ID is not None
        assert ATTR_AGGREGATE_TYPE is not None

    def test_all_event_attributes_importable(self):
        """All event attributes are importable from package."""
        from eventsource.observability import (
            ATTR_EVENT_COUNT,
            ATTR_EVENT_ID,
            ATTR_EVENT_TYPE,
        )

        assert ATTR_EVENT_ID is not None
        assert ATTR_EVENT_TYPE is not None
        assert ATTR_EVENT_COUNT is not None

    def test_all_version_attributes_importable(self):
        """All version attributes are importable from package."""
        from eventsource.observability import (
            ATTR_EXPECTED_VERSION,
            ATTR_FROM_VERSION,
            ATTR_VERSION,
        )

        assert ATTR_VERSION is not None
        assert ATTR_EXPECTED_VERSION is not None
        assert ATTR_FROM_VERSION is not None

    def test_all_tenant_actor_attributes_importable(self):
        """All tenant/actor attributes are importable from package."""
        from eventsource.observability import ATTR_ACTOR_ID, ATTR_TENANT_ID

        assert ATTR_TENANT_ID is not None
        assert ATTR_ACTOR_ID is not None

    def test_all_component_attributes_importable(self):
        """All component attributes are importable from package."""
        from eventsource.observability import (
            ATTR_HANDLER_COUNT,
            ATTR_HANDLER_NAME,
            ATTR_POSITION,
            ATTR_PROJECTION_NAME,
            ATTR_STREAM_ID,
        )

        assert ATTR_PROJECTION_NAME is not None
        assert ATTR_HANDLER_NAME is not None
        assert ATTR_HANDLER_COUNT is not None
        assert ATTR_STREAM_ID is not None
        assert ATTR_POSITION is not None

    def test_all_db_attributes_importable(self):
        """All database attributes are importable from package."""
        from eventsource.observability import (
            ATTR_DB_NAME,
            ATTR_DB_OPERATION,
            ATTR_DB_SYSTEM,
        )

        assert ATTR_DB_SYSTEM is not None
        assert ATTR_DB_NAME is not None
        assert ATTR_DB_OPERATION is not None

    def test_all_messaging_attributes_importable(self):
        """All messaging attributes are importable from package."""
        from eventsource.observability import (
            ATTR_MESSAGING_DESTINATION,
            ATTR_MESSAGING_OPERATION,
            ATTR_MESSAGING_SYSTEM,
        )

        assert ATTR_MESSAGING_SYSTEM is not None
        assert ATTR_MESSAGING_DESTINATION is not None
        assert ATTR_MESSAGING_OPERATION is not None

    def test_all_error_retry_attributes_importable(self):
        """All error/retry attributes are importable from package."""
        from eventsource.observability import ATTR_ERROR_TYPE, ATTR_RETRY_COUNT

        assert ATTR_RETRY_COUNT is not None
        assert ATTR_ERROR_TYPE is not None


class TestAttributeUsability:
    """Tests demonstrating practical attribute usage patterns."""

    def test_attributes_usable_as_dict_keys(self):
        """Attributes can be used as dictionary keys for span attributes."""
        span_attributes = {
            ATTR_AGGREGATE_ID: "123e4567-e89b-12d3-a456-426614174000",
            ATTR_EVENT_TYPE: "UserCreated",
            ATTR_VERSION: 1,
        }

        assert span_attributes[ATTR_AGGREGATE_ID] == "123e4567-e89b-12d3-a456-426614174000"
        assert span_attributes[ATTR_EVENT_TYPE] == "UserCreated"
        assert span_attributes[ATTR_VERSION] == 1

    def test_attributes_unique(self):
        """All attribute values should be unique."""
        all_values = [getattr(attributes, name) for name in attributes.__all__]
        assert len(all_values) == len(set(all_values)), "Attribute values should be unique"

    def test_no_trailing_dots_in_values(self):
        """Attribute values should not have trailing dots."""
        for name in attributes.__all__:
            value = getattr(attributes, name)
            assert not value.endswith("."), f"{name} should not end with a dot"

    def test_no_leading_dots_in_values(self):
        """Attribute values should not have leading dots."""
        for name in attributes.__all__:
            value = getattr(attributes, name)
            assert not value.startswith("."), f"{name} should not start with a dot"
