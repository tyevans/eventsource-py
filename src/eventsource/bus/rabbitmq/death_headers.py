"""Pure functions for reading RabbitMQ dead-letter (x-death) headers.

These helpers are pure: they read message headers only and have no side
effects. They are exposed as permanent static aliases on
``RabbitMQEventBus`` (``get_death_count``, ``is_from_dlq``, etc.) for
backward compatibility, and may also be called directly.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from aio_pika.abc import AbstractIncomingMessage


def get_death_count(message: AbstractIncomingMessage) -> int:
    """Get the number of times a message has been dead-lettered.

    RabbitMQ automatically adds an x-death header when a message is
    dead-lettered. This header contains an array of death records,
    each with a 'count' field indicating how many times the message
    has been dead-lettered for that reason/queue combination.

    Args:
        message: The incoming AMQP message

    Returns:
        Total death count across all death records, or 0 if never dead-lettered

    Example:
        >>> death_count = RabbitMQEventBus.get_death_count(message)
        >>> if death_count > 0:
        ...     print(f"Message has been dead-lettered {death_count} times")
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
    """Get the name of the queue where the message first died.

    RabbitMQ stores the original queue name in the x-first-death-queue
    header when a message is dead-lettered. This is useful for identifying
    the source of a failed message.

    Args:
        message: The incoming AMQP message

    Returns:
        The name of the original queue, or None if not dead-lettered

    Example:
        >>> queue_name = RabbitMQEventBus.get_first_death_queue(message)
        >>> if queue_name:
        ...     print(f"Message originally died in queue: {queue_name}")
    """
    headers = message.headers or {}
    first_death_queue = headers.get("x-first-death-queue")

    if first_death_queue is not None:
        return str(first_death_queue)

    return None


def get_first_death_reason(message: AbstractIncomingMessage) -> str | None:
    """Get the reason for the first death of a message.

    RabbitMQ stores the death reason in the x-first-death-reason header.
    Common reasons include:
    - 'rejected': Message was rejected by consumer
    - 'expired': Message TTL expired
    - 'maxlen': Queue max length exceeded

    Args:
        message: The incoming AMQP message

    Returns:
        The death reason, or None if not dead-lettered

    Example:
        >>> reason = RabbitMQEventBus.get_first_death_reason(message)
        >>> if reason == 'rejected':
        ...     print("Message was rejected by a handler")
    """
    headers = message.headers or {}
    first_death_reason = headers.get("x-first-death-reason")

    if first_death_reason is not None:
        return str(first_death_reason)

    return None


def get_first_death_exchange(message: AbstractIncomingMessage) -> str | None:
    """Get the exchange where the message first died.

    RabbitMQ stores the original exchange in the x-first-death-exchange
    header when a message is dead-lettered.

    Args:
        message: The incoming AMQP message

    Returns:
        The name of the original exchange, or None if not dead-lettered

    Example:
        >>> exchange = RabbitMQEventBus.get_first_death_exchange(message)
        >>> if exchange:
        ...     print(f"Message originally died from exchange: {exchange}")
    """
    headers = message.headers or {}
    first_death_exchange = headers.get("x-first-death-exchange")

    if first_death_exchange is not None:
        return str(first_death_exchange)

    return None


def get_original_routing_key(message: AbstractIncomingMessage) -> str | None:
    """Get the original routing key of a dead-lettered message.

    When a message is dead-lettered, the original routing key is preserved
    in the x-death header records. This method extracts the routing key
    from the first death record.

    Args:
        message: The incoming AMQP message

    Returns:
        The original routing key, or None if not dead-lettered

    Example:
        >>> routing_key = RabbitMQEventBus.get_original_routing_key(message)
        >>> if routing_key:
        ...     print(f"Original routing key: {routing_key}")
    """
    headers = message.headers or {}
    x_death = headers.get("x-death")

    if not x_death or not isinstance(x_death, list):
        return None

    # Get routing keys from first death record
    if x_death and isinstance(x_death[0], dict):
        routing_keys = x_death[0].get("routing-keys")
        if routing_keys and isinstance(routing_keys, list) and routing_keys:
            return str(routing_keys[0])

    return None


def is_from_dlq(message: AbstractIncomingMessage) -> bool:
    """Check if a message has been dead-lettered (came from DLQ).

    A message is considered to have come from DLQ if it has x-death
    headers, which RabbitMQ automatically adds when dead-lettering.

    This is useful for identifying messages that need special handling
    or have already failed processing.

    Args:
        message: The incoming AMQP message

    Returns:
        True if the message has been dead-lettered, False otherwise

    Example:
        >>> if RabbitMQEventBus.is_from_dlq(message):
        ...     print("This message was previously dead-lettered")
        ...     # Handle accordingly
    """
    headers = message.headers or {}
    x_death = headers.get("x-death")

    return x_death is not None and isinstance(x_death, list) and len(x_death) > 0


def get_death_info(message: AbstractIncomingMessage) -> dict[str, Any]:
    """Get comprehensive death information for a message.

    Extracts all death-related headers into a single dictionary for
    easy access and logging. This is useful for debugging and
    understanding message failure history.

    Args:
        message: The incoming AMQP message

    Returns:
        Dictionary containing:
        - is_dead_lettered: Whether message was dead-lettered
        - death_count: Total death count
        - first_death_queue: Original queue name
        - first_death_reason: Death reason
        - first_death_exchange: Original exchange
        - original_routing_key: Original routing key
        - x_death: Raw x-death header (list of death records)

    Example:
        >>> info = RabbitMQEventBus.get_death_info(message)
        >>> if info['is_dead_lettered']:
        ...     print(f"Message died {info['death_count']} time(s)")
        ...     print(f"Reason: {info['first_death_reason']}")
    """
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
