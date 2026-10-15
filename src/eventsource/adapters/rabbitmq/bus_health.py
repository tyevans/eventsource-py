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
    from eventsource.adapters.rabbitmq.config import RabbitMQEventBusConfig
    from eventsource.adapters.rabbitmq.connection import RabbitMQConnectionManager
    from eventsource.adapters.rabbitmq.consumer import RabbitMQConsumer
    from eventsource.adapters.rabbitmq.topology import RabbitMQTopology


class RabbitMQEventBusHealthMixin:
    """Mixin providing health check, queue info, and statistics for RabbitMQEventBus."""

    _stats: RabbitMQEventBusStats
    _config: RabbitMQEventBusConfig
    _topology: RabbitMQTopology
    _connection_manager: RabbitMQConnectionManager
    _consumer: RabbitMQConsumer
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
        """Get current statistics (method form)."""
        return self._stats

    def get_stats_dict(self) -> dict[str, Any]:
        """Get statistics as a dictionary suitable for JSON serialization and logging."""
        uptime_seconds: float | None = None
        if self._stats.connected_at is not None:
            uptime_delta = datetime.now(UTC) - self._stats.connected_at
            uptime_seconds = uptime_delta.total_seconds()

        return {
            "events_published": self._stats.events_published,
            "events_consumed": self._stats.events_consumed,
            "events_processed_success": self._stats.events_processed_success,
            "events_processed_failed": self._stats.events_processed_failed,
            "messages_sent_to_dlq": self._stats.messages_sent_to_dlq,
            "handler_errors": self._stats.handler_errors,
            "reconnections": self._stats.reconnections,
            "publish_confirms": self._stats.publish_confirms,
            "publish_returns": self._stats.publish_returns,
            "batch_publishes": self._stats.batch_publishes,
            "batch_events_published": self._stats.batch_events_published,
            "batch_partial_failures": self._stats.batch_partial_failures,
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
            "is_connected": self.is_connected,
            "is_consuming": self.is_consuming,
            "uptime_seconds": uptime_seconds,
        }

    def reset_stats(self) -> None:
        """Reset all statistics to initial values while preserving connected_at if connected."""
        connected_at = self._stats.connected_at if self._connection_manager._connected else None
        self._stats = RabbitMQEventBusStats(connected_at=connected_at)
        self._connection_manager._stats = self._stats
        self._logger.info("Statistics reset")

    async def get_queue_info(self) -> QueueInfo:
        """Get information about the consumer queue via passive queue declaration."""
        if not self._connection_manager._connected or not self._connection_manager._channel:
            return QueueInfo(
                name=self._config.queue_name,
                message_count=0,
                consumer_count=0,
                state="error",
                error="Not connected to RabbitMQ",
            )

        queue_info = await self._topology.queue_health(self._config.queue_name)
        if queue_info is None:
            return QueueInfo(
                name=self._config.queue_name,
                message_count=0,
                consumer_count=0,
                state="error",
                error="Not connected to RabbitMQ",
            )
        return queue_info

    async def health_check(self) -> HealthCheckResult:
        """Perform a comprehensive health check of the event bus."""
        connection_slice = self._connection_manager.health_slice()
        healthy = bool(connection_slice["healthy"])
        error_messages: list[str] = list(connection_slice["errors"])
        connection_status = connection_slice["connection_status"]
        channel_status = connection_slice["channel_status"]

        channel = self._connection_manager._channel
        consumer_queue = self._topology.consumer_queue

        # Check consumer queue accessibility (passive declare owned by RabbitMQTopology)
        queue_status = "not_initialized"
        if consumer_queue and channel and not channel.is_closed:
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
        elif not consumer_queue:
            queue_status = "not_initialized"
            # Don't mark as unhealthy if we simply haven't connected yet
            if self._connection_manager._connected:
                healthy = False
                error_messages.append("Consumer queue not initialized")

        # Check DLQ status if enabled (passive declare owned by RabbitMQTopology)
        dlq_status: str | None = None
        if self._config.enable_dlq:
            if channel and not channel.is_closed:
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
            "consuming": self._consumer._consuming,
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
