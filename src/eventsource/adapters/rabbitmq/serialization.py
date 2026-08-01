"""Pure event <-> AMQP message conversion for the RabbitMQ event bus.

These functions take their collaborators as explicit parameters instead of
reading them off ``self`` so they can be tested and reasoned about in
isolation from the god-class ``RabbitMQEventBus``. ``RabbitMQEventBus`` keeps
thin private wrappers that delegate here.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from eventsource.events.base import DomainEvent

if TYPE_CHECKING:
    from aio_pika.abc import AbstractIncomingMessage

# Optional aio-pika import - fail gracefully if not installed. Mirrors the
# guard-safe import pattern in bus.py; this module legitimately needs the
# runtime names (not just typing) because it constructs Message objects.
try:
    from aio_pika import DeliveryMode, Message

    RABBITMQ_AVAILABLE = True
except ImportError:
    RABBITMQ_AVAILABLE = False
    Message = None  # type: ignore[assignment, misc]
    DeliveryMode = None  # type: ignore[assignment, misc]

# OpenTelemetry propagation import - kept separate for distributed tracing
# context, mirroring bus.py's guard.
try:
    from opentelemetry.propagate import inject

    PROPAGATION_AVAILABLE = True
except ImportError:
    inject = None  # type: ignore[assignment]
    PROPAGATION_AVAILABLE = False


def get_event_field_default(event_type: type[DomainEvent], field_name: str, default: str) -> str:
    """Get the default value for a field from a DomainEvent subclass.

    This method handles both regular class attributes and Pydantic model
    field defaults. Pydantic stores field defaults in model_fields rather
    than as class attributes.

    Args:
        event_type: The DomainEvent subclass to inspect.
        field_name: The name of the field to get the default for.
        default: The fallback value if the field has no default.

    Returns:
        The default value for the field, or the provided default.
    """
    # First, check if it's a Pydantic model with model_fields
    if hasattr(event_type, "model_fields"):
        field_info = event_type.model_fields.get(field_name)
        if field_info is not None and field_info.default is not None:
            return str(field_info.default)

    # Fall back to getattr for non-Pydantic classes or missing fields
    return str(getattr(event_type, field_name, default))


def get_routing_key(event: DomainEvent) -> str:
    """Generate routing key for an event.

    Creates a routing key in the format {aggregate_type}.{event_type}
    for use with RabbitMQ topic exchanges. This allows consumers to
    subscribe to specific event types or aggregate types using wildcards.

    Examples:
        - Order.OrderCreated -> matches "Order.*" or "*.OrderCreated"
        - User.UserRegistered -> matches "User.*" or "#.Registered"

    Args:
        event: The domain event to generate a routing key for

    Returns:
        Routing key string in format "{aggregate_type}.{event_type}"
    """
    return f"{event.aggregate_type}.{event.event_type}"


def serialize_event(event: DomainEvent) -> tuple[bytes, dict[str, Any]]:
    """Serialize a domain event to JSON bytes and message headers.

    Converts a DomainEvent to a JSON-encoded byte string and extracts
    message metadata as headers for AMQP message properties.

    The JSON body contains the full event data serialized via Pydantic's
    model_dump_json() method, which handles UUID and datetime serialization.

    Headers include event metadata for:
    - Message routing and filtering (event_type, aggregate_type)
    - Event identification (aggregate_id, aggregate_version)
    - Retry tracking (x-retry-count)
    - Correlation/causation tracking (optional)
    - Multi-tenancy support (tenant_id, optional)

    Args:
        event: The domain event to serialize

    Returns:
        Tuple of (body_bytes, headers_dict):
        - body_bytes: UTF-8 encoded JSON representation of the event
        - headers_dict: Message headers for AMQP properties

    Example:
        >>> body, headers = serialize_event(order_created_event)
        >>> headers["event_type"]
        'OrderCreated'
        >>> headers["aggregate_type"]
        'Order'
    """
    # Serialize event to JSON bytes
    body = event.model_dump_json().encode("utf-8")

    # Build headers with event metadata
    headers: dict[str, Any] = {
        "event_type": event.event_type,
        "aggregate_type": event.aggregate_type,
        "aggregate_id": str(event.aggregate_id),
        "aggregate_version": event.aggregate_version,
        "x-retry-count": 0,
    }

    # Optional headers - only include if present
    if event.tenant_id:
        headers["tenant_id"] = str(event.tenant_id)
    if event.correlation_id:
        headers["correlation_id"] = str(event.correlation_id)
    if event.causation_id:
        headers["causation_id"] = str(event.causation_id)

    return body, headers


def create_message(event: DomainEvent) -> Message:
    """Create an AMQP message from a domain event.

    Creates a fully configured aio-pika Message with:
    - JSON body from serialized event
    - Appropriate content type and encoding
    - Persistent delivery mode for durability
    - Event metadata in headers
    - Message ID and timestamp from event

    This is a convenience function that combines serialize_event with
    Message construction for use in publish operations.

    Args:
        event: The domain event to convert to a message

    Returns:
        aio_pika.Message ready for publishing

    Example:
        >>> message = create_message(order_created_event)
        >>> message.content_type
        'application/json'
        >>> message.delivery_mode
        DeliveryMode.PERSISTENT
    """
    body, headers = serialize_event(event)

    return Message(
        body=body,
        content_type="application/json",
        content_encoding="utf-8",
        delivery_mode=DeliveryMode.PERSISTENT,
        message_id=str(event.event_id),
        timestamp=event.occurred_at,
        headers=headers,
    )


def create_message_with_tracing(
    event: DomainEvent,
    span: Any = None,
) -> Message:
    """Create an AMQP message with optional trace context injection.

    Similar to create_message but additionally injects OpenTelemetry
    trace context into message headers when a span is provided and
    tracing is available. This enables distributed tracing correlation
    across publish/consume operations.

    The trace context is injected using W3C Trace Context format
    (traceparent, tracestate headers) via OpenTelemetry's propagate.inject().

    Args:
        event: The domain event to convert to a message
        span: Optional OpenTelemetry span to extract trace context from.
             If None or tracing is not available, creates message without
             trace context (equivalent to create_message).

    Returns:
        aio_pika.Message ready for publishing with trace context in headers

    Example:
        >>> span = tracer.start_span("publish") if tracer else None
        >>> message = create_message_with_tracing(event, span)
    """
    body, headers = serialize_event(event)

    # Inject trace context into headers if span is active and propagation is available
    if span and PROPAGATION_AVAILABLE and inject is not None:
        inject(headers)

    return Message(
        body=body,
        content_type="application/json",
        content_encoding="utf-8",
        delivery_mode=DeliveryMode.PERSISTENT,
        message_id=str(event.event_id),
        timestamp=event.occurred_at,
        headers=headers,
    )


def deserialize_event(
    message: AbstractIncomingMessage,
    resolve_event_class: Callable[[str], type[DomainEvent] | None],
    logger: logging.Logger,
) -> DomainEvent | None:
    """Deserialize an AMQP message to a domain event.

    Extracts the event type from message headers, looks up the
    corresponding event class from the registry, and deserializes
    the JSON body to reconstruct the domain event.

    Uses the given event class resolver to resolve event type names to
    their corresponding Python classes.

    Args:
        message: Incoming AMQP message with headers and body
        resolve_event_class: Callable resolving an event type name to its
            DomainEvent subclass, or None if unknown.
        logger: Logger to use for warnings/errors during deserialization.

    Returns:
        Deserialized DomainEvent instance, or None if:
        - Message is missing event_type header
        - Event type is not found in registry
        - Deserialization fails (malformed JSON, validation error)

    Logs:
        - WARNING: Missing event_type header
        - WARNING: Unknown event type
        - ERROR: Deserialization failure with exception details
    """
    # Get event type from headers
    headers = message.headers or {}
    event_type_name = headers.get("event_type")

    if not event_type_name:
        logger.warning(
            "Message missing event_type header",
            extra={"message_id": message.message_id},
        )
        return None

    # Look up event class
    event_type_str = str(event_type_name)
    event_class = resolve_event_class(event_type_str)
    if event_class is None:
        logger.warning(
            f"Unknown event type: {event_type_str}",
            extra={
                "event_type": event_type_str,
                "message_id": message.message_id,
            },
        )
        return None

    # Deserialize from JSON body
    try:
        body = message.body.decode("utf-8")
        return event_class.model_validate_json(body)
    except Exception as e:
        logger.error(
            f"Failed to deserialize event: {e}",
            exc_info=True,
            extra={
                "event_type": event_type_name,
                "message_id": message.message_id,
                "error": str(e),
                "error_type": type(e).__name__,
            },
        )
        return None
