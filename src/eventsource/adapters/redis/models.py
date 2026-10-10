"""Domain models and stats for Redis event bus.

Governed by:
- ADR-0002 (<500 lines per module)
- ADR-0007 (Hexagonal Architecture)
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class RedisEventBusStats:
    """Statistics for Redis event bus operations.

    Attributes:
        events_published: Total events published
        events_consumed: Total events consumed
        events_processed_success: Events processed successfully
        events_processed_failed: Events that failed processing
        messages_recovered: Messages recovered from pending
        messages_sent_to_dlq: Messages sent to dead letter queue
        handler_errors: Total handler errors
        reconnections: Number of reconnection attempts
    """

    events_published: int = 0
    events_consumed: int = 0
    events_processed_success: int = 0
    events_processed_failed: int = 0
    messages_recovered: int = 0
    messages_sent_to_dlq: int = 0
    handler_errors: int = 0
    reconnections: int = 0


__all__ = ["RedisEventBusStats"]
