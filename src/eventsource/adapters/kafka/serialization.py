"""Event serializer base class for the Kafka event bus."""

from __future__ import annotations

from eventsource.adapters.kafka.models import DeserializationError
from eventsource.events.base import DomainEvent


class EventSerializer:
    """Base class for event serializers.

    This class defines the interface for serializing and deserializing events.
    Subclass this to implement custom serialization formats (Avro, Protobuf, etc.)
    or to integrate with schema registries.

    The default implementation uses JSON serialization via Pydantic.

    Example - Custom Avro Serializer:
        >>> class AvroEventSerializer(EventSerializer):
        ...     def __init__(self, schema_registry_url: str):
        ...         self._registry = SchemaRegistryClient(schema_registry_url)
        ...
        ...     def serialize(self, event: DomainEvent) -> bytes:
        ...         schema = self._get_schema(event.event_type)
        ...         return self._avro_serialize(event, schema)
        ...
        ...     def deserialize(
        ...         self,
        ...         data: bytes,
        ...         event_type: str,
        ...         event_class: type[DomainEvent],
        ...     ) -> DomainEvent:
        ...         schema = self._get_schema(event_type)
        ...         return self._avro_deserialize(data, schema, event_class)
    """

    def serialize(self, event: DomainEvent) -> bytes:
        """Serialize a domain event to bytes.

        Args:
            event: The event to serialize.

        Returns:
            The serialized event as bytes.

        Raises:
            ValueError: If serialization fails.
        """
        return event.model_dump_json().encode("utf-8")

    def deserialize(
        self,
        data: bytes,
        event_type: str,
        event_class: type[DomainEvent],
    ) -> DomainEvent:
        """Deserialize bytes to a domain event.

        Args:
            data: The serialized event data.
            event_type: The event type name (for logging/debugging).
            event_class: The event class to deserialize into.

        Returns:
            The deserialized DomainEvent.

        Raises:
            DeserializationError: If deserialization fails.
        """
        try:
            return event_class.model_validate_json(data)
        except Exception as e:
            raise DeserializationError(f"Failed to deserialize {event_type}: {e}") from e

    def content_type(self) -> str:
        """Get the content type for this serializer.

        Returns:
            The MIME content type string.
        """
        return "application/json"
