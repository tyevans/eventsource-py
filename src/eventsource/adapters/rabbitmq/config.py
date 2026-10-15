"""Configuration for the RabbitMQ event bus."""

from __future__ import annotations

import socket
import ssl
import uuid
from dataclasses import dataclass
from typing import Any


@dataclass
class RabbitMQEventBusConfig:
    """Configuration for RabbitMQ event bus connection, topology, DLQ, and retry behavior."""

    rabbitmq_url: str = "amqp://guest:guest@localhost:5672/"
    exchange_name: str = "events"
    exchange_type: str = "topic"  # topic, direct, fanout, headers
    consumer_group: str = "default"
    consumer_name: str | None = None
    prefetch_count: int = 10
    max_retries: int = 3
    enable_dlq: bool = True
    dlq_exchange_suffix: str = "_dlq"
    dlq_message_ttl: int | None = None
    dlq_max_length: int | None = None
    durable: bool = True
    auto_delete: bool = False
    reconnect_delay: float = 1.0
    heartbeat: int = 60
    enable_tracing: bool = True
    ssl_options: dict[str, Any] | None = None
    ssl_context: ssl.SSLContext | None = None
    verify_ssl: bool = True
    ca_file: str | None = None
    cert_file: str | None = None
    key_file: str | None = None
    retry_base_delay: float = 1.0
    retry_max_delay: float = 60.0
    retry_jitter: float = 0.1
    shutdown_timeout: float = 30.0
    routing_key_pattern: str | None = None
    publish_chunk_size: int = 100
    max_concurrent_publishes: int = 10

    def __post_init__(self) -> None:
        """Generate consumer name ({hostname}-{uuid[:8]}) if not provided."""
        if self.consumer_name is None:
            self.consumer_name = f"{socket.gethostname()}-{str(uuid.uuid4())[:8]}"

    @property
    def queue_name(self) -> str:
        """Get the consumer queue name ({exchange_name}.{consumer_group})."""
        return f"{self.exchange_name}.{self.consumer_group}"

    @property
    def dlq_exchange_name(self) -> str:
        """Get the dead letter exchange name ({exchange_name}{dlq_exchange_suffix})."""
        return f"{self.exchange_name}{self.dlq_exchange_suffix}"

    @property
    def dlq_queue_name(self) -> str:
        """Get the dead letter queue name ({queue_name}.dlq)."""
        return f"{self.queue_name}.dlq"

    def get_effective_routing_key(self) -> str:
        """Get the effective routing key pattern for queue binding."""
        if self.routing_key_pattern is not None:
            return self.routing_key_pattern
        exchange_type_lower = self.exchange_type.lower()
        if exchange_type_lower == "direct":
            return self.queue_name
        if exchange_type_lower in ("fanout", "headers"):
            return ""
        return "#"
