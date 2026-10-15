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

from eventsource.domain.event import DomainEvent

if TYPE_CHECKING:
    from aio_pika.abc import AbstractIncomingMessage

# Optional aio-pika import - fail gracefully if not installed.
try:
    from aio_pika import DeliveryMode, Message
except ImportError:
    Message = None  # type: ignore[assignment, misc]
    DeliveryMode = None  # type: ignore[assignment, misc]

# OpenTelemetry propagation import for distributed tracing context.
try:
    from opentelemetry.propagate import inject

    PROPAGATION_AVAILABLE = True
except ImportError:
    inject = None  # type: ignore[assignment]
    PROPAGATION_AVAILABLE = False


def get_event_field_default(event_type: type[DomainEvent], field_name: str, default: str) -> str:
    """Get the default value for a field from a DomainEvent subclass."""
    if hasattr(event_type, "model_fields"):
        field_info = event_type.model_fields.get(field_name)
        if field_info is not None and field_info.default is not None:
            return str(field_info.default)

    return str(getattr(event_type, field_name, default))


def get_routing_key(event: DomainEvent) -> str:
    """Generate routing key in format '{aggregate_type}.{event_type}' for an event."""
    return f"{event.aggregate_type}.{event.event_type}"


def serialize_event(event: DomainEvent) -> tuple[bytes, dict[str, Any]]:
    """Serialize a domain event to UTF-8 JSON bytes and AMQP message headers."""
    body = event.model_dump_json().encode("utf-8")

    headers: dict[str, Any] = {
        "event_type": event.event_type,
        "aggregate_type": event.aggregate_type,
        "aggregate_id": str(event.aggregate_id),
        "aggregate_version": event.aggregate_version,
        "x-retry-count": 0,
    }

    if event.tenant_id:
        headers["tenant_id"] = str(event.tenant_id)
    if event.correlation_id:
        headers["correlation_id"] = str(event.correlation_id)
    if event.causation_id:
        headers["causation_id"] = str(event.causation_id)

    return body, headers


def create_message(event: DomainEvent) -> Message:
    """Create a persistent AMQP message from a domain event."""
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
    """Create an AMQP message with optional OpenTelemetry trace context injection."""
    body, headers = serialize_event(event)

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
    """Deserialize an AMQP message to a DomainEvent, or None on missing/unknown/invalid data."""
    headers = message.headers or {}
    event_type_name = headers.get("event_type")

    if not event_type_name:
        logger.warning(
            "Message missing event_type header",
            extra={"message_id": message.message_id},
        )
        return None

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
