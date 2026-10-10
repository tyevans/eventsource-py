"""
Health, queue info, and statistics operations mixin for RabbitMQEventBus.

Provides health checks, queue inspection, and statistics gathering.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from eventsource.adapters.rabbitmq.models import (
    HealthCheckResult,
    QueueInfo,
    RabbitMQEventBusStats,
)

if TYPE_CHECKING:
    from aio_pika.abc import AbstractQueue, AbstractRobustChannel

    from eventsource.adapters.rabbitmq.config import RabbitMQEventBusConfig
    from eventsource.adapters.rabbitmq.connection import RabbitMQConnectionManager
    from eventsource.adapters.rabbitmq.topology import RabbitMQTopology


class RabbitMQEventBusHealthMixin:
    """Mixin providing health check, queue info, and statistics for RabbitMQEventBus."""

    _stats: RabbitMQEventBusStats
    _config: RabbitMQEventBusConfig
    _connected: bool
    _channel: AbstractRobustChannel | None
    _topology: RabbitMQTopology
    _connection_manager: RabbitMQConnectionManager
    _consumer_queue: AbstractQueue | None
    _consuming: bool
    _logger: logging.Logger

    if TYPE_CHECKING:

        @property
        def is_connected(self) -> bool: ...
        @property
        def is_consuming(self) -> bool: ...

    @property
    def stats(self) -> RabbitMQEventBusStats:
        """Get current statistics."""
        return self._stats

    def get_stats(self) -> RabbitMQEventBusStats:
        """Get current statistics (method form).

        This method provides an alternative way to access statistics,
        useful for consistency with other interfaces that expect a method
        rather than a property.

        Returns:
            RabbitMQEventBusStats with current values.
        """
        return self._stats

    def get_stats_dict(self) -> dict[str, Any]:
        """Get statistics as a dictionary.

        Converts all statistics to a dictionary format suitable for
        JSON serialization and logging. Includes counters, timing
        information, connection state, and uptime calculation.

        Returns:
            Dictionary with all statistics including:
            - Counter fields (events_published, events_consumed, etc.)
            - Timing fields as ISO format strings (last_publish_at, etc.)
            - Connection state (is_connected, is_consuming)
            - Uptime in seconds (None if not connected)
            - Queue depth if available

        Example:
            >>> bus = RabbitMQEventBus(config=config)
            >>> await bus.connect()
            >>> stats_dict = bus.get_stats_dict()
            >>> import json
            >>> print(json.dumps(stats_dict, indent=2))
        """
        # Calculate uptime if connected
        uptime_seconds: float | None = None
        if self._stats.connected_at is not None:
            uptime_delta = datetime.now(UTC) - self._stats.connected_at
            uptime_seconds = uptime_delta.total_seconds()

        return {
            # Counters
            "events_published": self._stats.events_published,
            "events_consumed": self._stats.events_consumed,
            "events_processed_success": self._stats.events_processed_success,
            "events_processed_failed": self._stats.events_processed_failed,
            "messages_sent_to_dlq": self._stats.messages_sent_to_dlq,
            "handler_errors": self._stats.handler_errors,
            "reconnections": self._stats.reconnections,
            "publish_confirms": self._stats.publish_confirms,
            "publish_returns": self._stats.publish_returns,
            # Batch publishing counters
            "batch_publishes": self._stats.batch_publishes,
            "batch_events_published": self._stats.batch_events_published,
            "batch_partial_failures": self._stats.batch_partial_failures,
            # Timing (ISO format strings for JSON serialization)
            "last_publish_at": (
                self._stats.last_publish_at.isoformat() if self._stats.last_publish_at else None
            ),
            "last_consume_at": (
                self._stats.last_consume_at.isoformat() if self._stats.last_consume_at else None
            ),
            "last_error_at": (
                self._stats.last_error_at.isoformat() if self._stats.last_error_at else None
            ),
            "connected_at": (
                self._stats.connected_at.isoformat() if self._stats.connected_at else None
            ),
            # Connection state
            "is_connected": self.is_connected,
            "is_consuming": self.is_consuming,
            # Uptime in seconds
            "uptime_seconds": uptime_seconds,
        }

    def reset_stats(self) -> None:
        """Reset all statistics to initial values.

        Creates a new RabbitMQEventBusStats instance with default values,
        effectively resetting all counters to zero and all timestamps to None.

        Note:
            This does not affect connection state or other bus state,
            only the statistics tracking. The connected_at timestamp
            is also reset, which will affect uptime calculation until
            the next connection is established.

        Example:
            >>> bus = RabbitMQEventBus(config=config)
            >>> await bus.connect()
            >>> await bus.publish([event])
            >>> print(bus.stats.events_published)  # 1
            >>> bus.reset_stats()
            >>> print(bus.stats.events_published)  # 0
        """
        # Preserve connected_at if still connected to maintain accurate uptime
        connected_at = self._stats.connected_at if self._connected else None
        self._stats = RabbitMQEventBusStats(connected_at=connected_at)
        # Keep the connection manager's stats reference in sync -- it was
        # handed the original instance at construction time.
        self._connection_manager._stats = self._stats
        self._logger.info("Statistics reset")

    async def get_queue_info(self) -> QueueInfo:
        """Get information about the consumer queue.

        Retrieves queue statistics using passive queue declaration, which
        queries the queue state without modifying it. This is safe to call
        at any time and does not affect the queue or its messages.

        Returns:
            QueueInfo object containing:
            - name: Queue name
            - message_count: Number of messages waiting in the queue
            - consumer_count: Number of active consumers
            - state: "running", "idle", "unknown", or "error"
            - error: Error message if state is "error"

        Note:
            If not connected or channel is not initialized, returns a
            QueueInfo with state="error" and appropriate error message.

        Example:
            >>> info = await bus.get_queue_info()
            >>> print(f"Queue {info.name} has {info.message_count} messages")
            >>> print(f"State: {info.state}, Consumers: {info.consumer_count}")
        """
        # Handle not connected state
        if not self._connected or not self._channel:
            return QueueInfo(
                name=self._config.queue_name,
                message_count=0,
                consumer_count=0,
                state="error",
                error="Not connected to RabbitMQ",
            )

        # Delegate the passive-declare check to RabbitMQTopology.
        queue_info = await self._topology.queue_health(self._config.queue_name)
        if queue_info is None:
            # No live channel to query -- same "not connected" shape as above.
            return QueueInfo(
                name=self._config.queue_name,
                message_count=0,
                consumer_count=0,
                state="error",
                error="Not connected to RabbitMQ",
            )
        return queue_info

    async def health_check(self) -> HealthCheckResult:
        """Perform a comprehensive health check of the event bus.

        Checks the status of:
        - RabbitMQ connection
        - AMQP channel
        - Consumer queue accessibility
        - Dead letter queue (if enabled)

        This method is safe to call frequently and does not modify any
        queue or exchange state.

        Returns:
            HealthCheckResult object containing:
            - healthy: True if all components are operational
            - connection_status: "connected", "disconnected", or "closed"
            - channel_status: "open", "closed", or "not_initialized"
            - queue_status: "accessible", "inaccessible", "not_initialized", or "error: ..."
            - dlq_status: Status of DLQ or "disabled" if DLQ not enabled
            - error: Error message if unhealthy
            - details: Additional configuration and state information

        Example:
            >>> result = await bus.health_check()
            >>> if result.healthy:
            ...     print("Event bus is healthy")
            ... else:
            ...     print(f"Unhealthy: {result.error}")
            ...     print(f"Details: {result.details}")
        """
        # Connection/channel checks are owned by RabbitMQConnectionManager.
        connection_slice = self._connection_manager.health_slice()
        healthy = bool(connection_slice["healthy"])
        error_messages: list[str] = list(connection_slice["errors"])
        connection_status = connection_slice["connection_status"]
        channel_status = connection_slice["channel_status"]

        # Check consumer queue accessibility (passive declare owned by RabbitMQTopology)
        queue_status = "not_initialized"
        if self._consumer_queue and self._channel and not self._channel.is_closed:
            queue_info = await self._topology.queue_health(self._config.queue_name)
            if queue_info is None:
                # Channel closed between our gate above and the topology call --
                # the old inline code would have hit declare_queue's own
                # exception path in this race, so treat it the same way.
                queue_error = "Channel not initialized"
                queue_status = f"error: {queue_error}"
                healthy = False
                error_messages.append(f"Queue check failed: {queue_error}")
            elif queue_info.state == "error":
                queue_status = f"error: {queue_info.error}"
                healthy = False
                error_messages.append(f"Queue check failed: {queue_info.error}")
            else:
                queue_status = "accessible"
        elif not self._consumer_queue:
            queue_status = "not_initialized"
            # Don't mark as unhealthy if we simply haven't connected yet
            if self._connected:
                healthy = False
                error_messages.append("Consumer queue not initialized")

        # Check DLQ status if enabled (passive declare owned by RabbitMQTopology)
        dlq_status: str | None = None
        if self._config.enable_dlq:
            if self._channel and not self._channel.is_closed:
                dlq_info = await self._topology.queue_health(self._config.dlq_queue_name)
                if dlq_info is None:
                    # Channel closed between our gate above and the topology
                    # call -- same race as the consumer-queue branch above.
                    dlq_error = "Channel not initialized"
                    dlq_status = f"error: {dlq_error}"
                    self._logger.warning(
                        f"DLQ health check failed: {dlq_error}",
                        extra={"dlq_queue": self._config.dlq_queue_name},
                    )
                elif dlq_info.state == "error":
                    dlq_status = f"error: {dlq_info.error}"
                    # DLQ errors don't make the overall bus unhealthy
                    # but we log them
                    self._logger.warning(
                        f"DLQ health check failed: {dlq_info.error}",
                        extra={"dlq_queue": self._config.dlq_queue_name},
                    )
                else:
                    dlq_status = "accessible"
            else:
                dlq_status = "inaccessible"
        else:
            dlq_status = "disabled"

        # Build details dictionary
        details: dict[str, Any] = {
            "exchange": self._config.exchange_name,
            "queue": self._config.queue_name,
            "consumer_group": self._config.consumer_group,
            "consuming": self._consuming,
            "dlq_enabled": self._config.enable_dlq,
        }

        if self._config.enable_dlq:
            details["dlq_queue"] = self._config.dlq_queue_name

        # Add stats summary
        details["stats"] = {
            "events_published": self._stats.events_published,
            "events_consumed": self._stats.events_consumed,
            "events_processed_success": self._stats.events_processed_success,
            "events_processed_failed": self._stats.events_processed_failed,
            "messages_sent_to_dlq": self._stats.messages_sent_to_dlq,
            "reconnections": self._stats.reconnections,
        }

        # Combine error messages
        error = "; ".join(error_messages) if error_messages else None

        self._logger.debug(
            f"Health check completed: healthy={healthy}",
            extra={
                "healthy": healthy,
                "connection_status": connection_status,
                "channel_status": channel_status,
                "queue_status": queue_status,
                "dlq_status": dlq_status,
            },
        )

        return HealthCheckResult(
            healthy=healthy,
            connection_status=connection_status,
            channel_status=channel_status,
            queue_status=queue_status,
            dlq_status=dlq_status,
            error=error,
            details=details,
        )
