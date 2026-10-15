"""Pure functions for reading RabbitMQ dead-letter (x-death) headers.

These helpers inspect message headers only and have no side effects. They are
exposed as static methods on ``RabbitMQEventBus`` and may also be called directly.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from aio_pika.abc import AbstractIncomingMessage


def get_death_count(message: AbstractIncomingMessage) -> int:
    """Get the total number of times a message has been dead-lettered.

    Sums the ``count`` field across all records in the ``x-death`` header,
    returning 0 if the message has never been dead-lettered.
    """
    headers = message.headers or {}
    x_death = headers.get("x-death")

    if not x_death or not isinstance(x_death, list):
        return 0

    total_count = 0
    for death_record in x_death:
        if isinstance(death_record, dict):
            count = death_record.get("count", 0)
            if isinstance(count, int):
                total_count += count

    return total_count


def get_first_death_queue(message: AbstractIncomingMessage) -> str | None:
    """Get the queue name where the message first died (``x-first-death-queue``)."""
    headers = message.headers or {}
    first_death_queue = headers.get("x-first-death-queue")

    if first_death_queue is not None:
        return str(first_death_queue)

    return None


def get_first_death_reason(message: AbstractIncomingMessage) -> str | None:
    """Get the reason for the first death of a message (``x-first-death-reason``)."""
    headers = message.headers or {}
    first_death_reason = headers.get("x-first-death-reason")

    if first_death_reason is not None:
        return str(first_death_reason)

    return None


def get_first_death_exchange(message: AbstractIncomingMessage) -> str | None:
    """Get the exchange where the message first died (``x-first-death-exchange``)."""
    headers = message.headers or {}
    first_death_exchange = headers.get("x-first-death-exchange")

    if first_death_exchange is not None:
        return str(first_death_exchange)

    return None


def get_original_routing_key(message: AbstractIncomingMessage) -> str | None:
    """Get the original routing key from the first ``x-death`` record."""
    headers = message.headers or {}
    x_death = headers.get("x-death")

    if not x_death or not isinstance(x_death, list):
        return None

    if x_death and isinstance(x_death[0], dict):
        routing_keys = x_death[0].get("routing-keys")
        if routing_keys and isinstance(routing_keys, list) and routing_keys:
            return str(routing_keys[0])

    return None


def is_from_dlq(message: AbstractIncomingMessage) -> bool:
    """Return True if the message has non-empty ``x-death`` headers."""
    headers = message.headers or {}
    x_death = headers.get("x-death")

    return x_death is not None and isinstance(x_death, list) and len(x_death) > 0


def get_death_info(message: AbstractIncomingMessage) -> dict[str, Any]:
    """Extract all dead-letter header fields into a dictionary."""
    headers = message.headers or {}

    return {
        "is_dead_lettered": is_from_dlq(message),
        "death_count": get_death_count(message),
        "first_death_queue": get_first_death_queue(message),
        "first_death_reason": get_first_death_reason(message),
        "first_death_exchange": get_first_death_exchange(message),
        "original_routing_key": get_original_routing_key(message),
        "x_death": headers.get("x-death"),
    }
