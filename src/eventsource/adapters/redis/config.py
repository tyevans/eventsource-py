"""Configuration and connection utilities for the Redis event bus.

Governed by:
- ADR-0002 (<500 lines per module)
- ADR-0007 (Hexagonal Architecture)
"""

from __future__ import annotations

import socket
import uuid
from dataclasses import dataclass
from typing import Any

# Optional Redis import - fail gracefully if not installed
try:
    import redis.asyncio as aioredis
    from redis.asyncio import Redis
    from redis.exceptions import ConnectionError as RedisConnectionError
    from redis.exceptions import ResponseError

    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False
    aioredis = None  # type: ignore[assignment]
    Redis = None  # type: ignore[assignment, misc]
    RedisConnectionError = Exception  # type: ignore[assignment, misc]
    ResponseError = Exception  # type: ignore[assignment, misc]

# This client connects with decode_responses=True, so every value Redis hands
# back is already `str`. redis-py's own annotations have to stay conservative
# (`bytes | str | int`), because that flag's effect is invisible to the type
# system. These aliases name the decoded shapes so the casts at each command
# boundary below stay narrow and self-documenting rather than becoming a
# scattering of per-line ignores.
DecodedFields = dict[str, str]
DecodedEntry = tuple[str, DecodedFields]
DecodedStreams = list[tuple[str, list[DecodedEntry]]]
DecodedPending = list[dict[str, Any]]


class RedisNotAvailableError(ImportError):
    """Raised when redis package is not installed."""

    def __init__(self) -> None:
        super().__init__(
            "Redis package is not installed. Install it with: pip install eventsource[redis]"
        )


@dataclass
class RedisEventBusConfig:
    """Configuration for Redis event bus.

    Attributes:
        redis_url: Redis connection URL (e.g., "redis://localhost:6379")
        stream_prefix: Prefix for Redis stream names (default: "events")
        consumer_group: Name of the consumer group (default: "default")
        consumer_name: Name of this consumer instance (auto-generated if None)
        stream_read_count: Maximum events to read per XREADGROUP call
            (forwarded as the ``count`` kwarg; default: 100). Also used as
            the default batch size for ``recover_pending_messages()``'s
            XPENDING/XCLAIM calls.
        block_ms: Milliseconds to block waiting for new messages (default: 5000)
        max_retries: Maximum retries before sending to DLQ (default: 3)
        pending_idle_ms: Minimum idle time before claiming pending messages (default: 60000)
        enable_dlq: Whether to enable dead letter queue (default: True)
        dlq_stream_suffix: Suffix for DLQ stream name (default: "_dlq")
        socket_timeout: Socket timeout in seconds (default: 5.0)
        socket_connect_timeout: Socket connection timeout in seconds (default: 5.0)
        enable_tracing: Enable OpenTelemetry tracing if available (default: True)
        retry_key_prefix: Prefix for retry count keys (default: "retry")
        retry_key_expiry_seconds: Expiry for retry count keys (default: 86400 = 24h)
        single_connection_client: Use single connection instead of pool (default: False).
    """

    redis_url: str = "redis://localhost:6379"
    stream_prefix: str = "events"
    consumer_group: str = "default"
    consumer_name: str | None = None
    stream_read_count: int = 100
    block_ms: int = 5000
    max_retries: int = 3
    pending_idle_ms: int = 60000  # 1 minute
    enable_dlq: bool = True
    dlq_stream_suffix: str = "_dlq"
    socket_timeout: float = 5.0
    socket_connect_timeout: float = 5.0
    enable_tracing: bool = True
    retry_key_prefix: str = "retry"
    retry_key_expiry_seconds: int = 86400  # 24 hours
    single_connection_client: bool = False

    def __post_init__(self) -> None:
        """Generate consumer name if not provided."""
        if self.consumer_name is None:
            hostname = socket.gethostname()
            unique_id = str(uuid.uuid4())[:8]
            self.consumer_name = f"{hostname}-{unique_id}"

    @property
    def stream_name(self) -> str:
        """Get the main stream name."""
        return f"{self.stream_prefix}:stream"

    @property
    def dlq_stream_name(self) -> str:
        """Get the dead letter queue stream name."""
        return f"{self.stream_prefix}:stream{self.dlq_stream_suffix}"

    def get_retry_key(self, message_id: str) -> str:
        """Get the Redis key for tracking retry count."""
        return f"{self.stream_prefix}:{self.retry_key_prefix}:{message_id}"


__all__ = [
    "REDIS_AVAILABLE",
    "DecodedEntry",
    "DecodedFields",
    "DecodedPending",
    "DecodedStreams",
    "RedisConnectionError",
    "RedisEventBusConfig",
    "RedisNotAvailableError",
    "ResponseError",
    "aioredis",
]
