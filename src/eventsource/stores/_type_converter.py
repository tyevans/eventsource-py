"""
Type conversion for event store serialization.

This module provides the TypeConverter protocol and DefaultTypeConverter
implementation for converting between JSON-stored strings and Python types
during event deserialization.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Protocol, runtime_checkable
from uuid import UUID


@runtime_checkable
class TypeConverter(Protocol):
    """
    Protocol for type conversion during event deserialization.

    Implementations handle converting string representations of UUIDs,
    datetimes, and other types back to their proper Python types when
    reading events from storage.

    This protocol uses structural subtyping - any class with matching
    method signatures satisfies the protocol without explicit inheritance.

    Example:
        >>> class MyConverter:
        ...     def convert_types(self, data: Any) -> Any:
        ...         return data  # No conversion
        ...     def is_uuid_field(self, key: str) -> bool:
        ...         return key.endswith("_uuid")
        ...     def is_datetime_field(self, key: str) -> bool:
        ...         return key.endswith("_timestamp")
        ...
        >>> isinstance(MyConverter(), TypeConverter)
        True
    """

    def convert_types(self, data: Any) -> Any:
        """
        Recursively convert string types to proper Python types.

        Traverses dictionaries and lists, converting UUID strings to
        uuid.UUID objects and datetime strings to datetime objects
        based on field name detection.

        Args:
            data: The data to convert. May be a dict, list, or scalar value.

        Returns:
            The converted data with the same structure but proper types.
            Original data is not modified; a new object is returned.

        Note:
            Invalid values (malformed UUIDs, datetimes) are preserved
            unchanged rather than raising exceptions.
        """
        ...

    def is_uuid_field(self, key: str) -> bool:
        """
        Determine if a field should be treated as a UUID.

        Args:
            key: The field name to check.

        Returns:
            True if the field's string value should be converted to UUID,
            False otherwise.
        """
        ...

    def is_datetime_field(self, key: str) -> bool:
        """
        Determine if a field should be treated as a datetime.

        Args:
            key: The field name to check.

        Returns:
            True if the field's string value should be converted to datetime,
            False otherwise.
        """
        ...


# Default UUID fields (always treated as UUID)
DEFAULT_UUID_FIELDS: frozenset[str] = frozenset(
    {
        "event_id",
        "aggregate_id",
        "tenant_id",
        "correlation_id",
        "causation_id",
        "template_id",
        "issuance_id",
        "user_id",
    }
)

# Default string ID fields (never treated as UUID even if ends with _id)
DEFAULT_STRING_ID_FIELDS: frozenset[str] = frozenset(
    {
        "actor_id",
        "issuer_id",
        "recipient_id",
        "invited_by",
        "assigned_by",
        "revoked_by",
        "deactivated_by",
        "reactivated_by",
        "removed_by",
    }
)


class DefaultTypeConverter:
    """
    Default implementation of TypeConverter with configurable UUID detection.

    Converts UUID and datetime strings back to proper Python types during
    event deserialization. Supports configurable field detection based on
    explicit field sets and/or automatic detection by field name patterns.

    Attributes:
        _uuid_fields: Fields always treated as UUIDs
        _string_id_fields: Fields never treated as UUIDs (exclusions)
        _auto_detect_uuid: Whether to auto-detect UUIDs by '_id' suffix

    Example:
        Default usage (uses built-in field sets + auto-detection):

        >>> converter = DefaultTypeConverter()
        >>> data = {"event_id": "550e8400-e29b-41d4-a716-446655440000"}
        >>> result = converter.convert_types(data)
        >>> isinstance(result["event_id"], UUID)
        True

        Custom UUID fields:

        >>> converter = DefaultTypeConverter(
        ...     uuid_fields={"custom_reference_id", "parent_id"},
        ... )

        Exclude string IDs from auto-detection:

        >>> converter = DefaultTypeConverter(
        ...     string_id_fields={"stripe_customer_id"},
        ... )

        Strict mode (no auto-detection, no defaults):

        >>> converter = DefaultTypeConverter.strict(
        ...     uuid_fields={"event_id", "aggregate_id"},
        ... )
    """

    def __init__(
        self,
        *,
        uuid_fields: set[str] | None = None,
        string_id_fields: set[str] | None = None,
        auto_detect_uuid: bool = True,
        use_defaults: bool = True,
    ) -> None:
        """
        Initialize the type converter.

        Args:
            uuid_fields: Additional field names to treat as UUIDs.
                When use_defaults=True, these are added to DEFAULT_UUID_FIELDS.
                When use_defaults=False, only these fields are used.
            string_id_fields: Field names that should NOT be treated as UUIDs,
                even if they match UUID patterns (e.g., end in '_id').
                When use_defaults=True, these are added to DEFAULT_STRING_ID_FIELDS.
            auto_detect_uuid: If True (default), fields ending in '_id' are
                treated as UUIDs unless in string_id_fields.
                Set to False for explicit control only.
            use_defaults: If True (default), merge provided fields with defaults.
                If False, only use explicitly provided fields (strict mode).
        """
        if use_defaults:
            self._uuid_fields: frozenset[str] = (
                DEFAULT_UUID_FIELDS | frozenset(uuid_fields) if uuid_fields else DEFAULT_UUID_FIELDS
            )
            self._string_id_fields: frozenset[str] = (
                DEFAULT_STRING_ID_FIELDS | frozenset(string_id_fields)
                if string_id_fields
                else DEFAULT_STRING_ID_FIELDS
            )
        else:
            self._uuid_fields = frozenset(uuid_fields) if uuid_fields else frozenset()
            self._string_id_fields = (
                frozenset(string_id_fields) if string_id_fields else frozenset()
            )

        self._auto_detect_uuid = auto_detect_uuid

    @classmethod
    def strict(cls, uuid_fields: set[str]) -> DefaultTypeConverter:
        """
        Create converter with explicit UUID field list only (no auto-detection).

        Use this when you want full control over which fields are UUIDs.
        Only the explicitly provided uuid_fields will be treated as UUIDs;
        no auto-detection based on field name patterns will occur.

        Args:
            uuid_fields: Exact set of fields to treat as UUIDs

        Returns:
            DefaultTypeConverter with strict UUID detection

        Example:
            >>> converter = DefaultTypeConverter.strict(
            ...     uuid_fields={"event_id", "aggregate_id", "tenant_id"},
            ... )
        """
        return cls(
            uuid_fields=uuid_fields,
            string_id_fields=set(),
            auto_detect_uuid=False,
            use_defaults=False,
        )

    def convert_types(self, data: Any) -> Any:
        """
        Recursively convert string types to proper Python types.

        Traverses dictionaries and lists, converting UUID strings to
        uuid.UUID objects and datetime strings to datetime objects
        based on field name detection.

        Args:
            data: The data to convert. May be a dict, list, or scalar value.

        Returns:
            The converted data with the same structure but proper types.
            Original data is not modified; a new object is returned.

        Note:
            Invalid values (malformed UUIDs, datetimes) are preserved
            unchanged rather than raising exceptions.
        """
        if isinstance(data, dict):
            result: dict[str, Any] = {}
            for key, value in data.items():
                # Convert UUID string fields
                if isinstance(value, str) and self.is_uuid_field(key):
                    try:
                        result[key] = UUID(value)
                    except (ValueError, AttributeError):
                        result[key] = value
                # Convert datetime strings
                elif isinstance(value, str) and self.is_datetime_field(key):
                    try:
                        result[key] = datetime.fromisoformat(value.replace("Z", "+00:00"))
                    except (ValueError, AttributeError):
                        result[key] = value
                # Recursively process nested structures
                elif isinstance(value, dict):
                    result[key] = self.convert_types(value)
                elif isinstance(value, list):
                    result[key] = [self.convert_types(item) for item in value]
                else:
                    result[key] = value
            return result
        elif isinstance(data, list):
            return [self.convert_types(item) for item in data]
        return data

    def is_uuid_field(self, key: str) -> bool:
        """
        Determine if a field should be treated as a UUID.

        Args:
            key: Field name to check

        Returns:
            True if field should be parsed/serialized as UUID

        The detection logic:
            1. If key is in explicit uuid_fields, return True
            2. If key is in string_id_fields, return False (exclusion)
            3. If auto_detect_uuid is True and key ends with '_id', return True
            4. Otherwise return False
        """
        # Explicit UUID fields always match
        if key in self._uuid_fields:
            return True

        # Excluded fields never match
        if key in self._string_id_fields:
            return False

        # Auto-detection based on suffix
        return self._auto_detect_uuid and key.endswith("_id")

    def is_datetime_field(self, key: str) -> bool:
        """
        Determine if a field should be treated as a datetime.

        Args:
            key: The field name to check.

        Returns:
            True if the field's string value should be converted to datetime,
            False otherwise.

        The detection logic:
            1. If key is "occurred_at", return True
            2. If key ends with "_at", return True
            3. Otherwise return False
        """
        return key == "occurred_at" or key.endswith("_at")
