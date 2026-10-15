"""Health, queue info, and statistics operations mixin for RabbitMQEventBus."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from eventsource.adapters.rabbitmq.models import HealthCheckResult, QueueInfo, RabbitMQEventBusStats

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
        s = self._stats
        uptime_seconds = (
            (datetime.now(UTC) - s.connected_at).total_seconds() if s.connected_at else None
        )
        return {
            "events_published": s.events_published,
            "events_consumed": s.events_consumed,
            "events_processed_success": s.events_processed_success,
            "events_processed_failed": s.events_processed_failed,
            "messages_sent_to_dlq": s.messages_sent_to_dlq,
            "handler_errors": s.handler_errors,
            "reconnections": s.reconnections,
            "publish_confirms": s.publish_confirms,
            "publish_returns": s.publish_returns,
            "batch_publishes": s.batch_publishes,
            "batch_events_published": s.batch_events_published,
            "batch_partial_failures": s.batch_partial_failures,
            "last_publish_at": s.last_publish_at.isoformat() if s.last_publish_at else None,
            "last_consume_at": s.last_consume_at.isoformat() if s.last_consume_at else None,
            "last_error_at": s.last_error_at.isoformat() if s.last_error_at else None,
            "connected_at": s.connected_at.isoformat() if s.connected_at else None,
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
        if self._connection_manager._connected and self._connection_manager._channel:
            queue_info = await self._topology.queue_health(self._config.queue_name)
            if queue_info is not None:
                return queue_info
        return QueueInfo(name=self._config.queue_name, message_count=0, consumer_count=0, state="error", error="Not connected to RabbitMQ")  # fmt: skip

    async def health_check(self) -> HealthCheckResult:
        """Perform a comprehensive health check of the event bus."""
        c_slice = self._connection_manager.health_slice()
        healthy, error_messages = bool(c_slice["healthy"]), list(c_slice["errors"])
        conn_status, chan_status = c_slice["connection_status"], c_slice["channel_status"]
        channel, consumer_queue = self._connection_manager._channel, self._topology.consumer_queue

        queue_status = "not_initialized"
        if consumer_queue and channel and not channel.is_closed:
            queue_info = await self._topology.queue_health(self._config.queue_name)
            if queue_info is None or queue_info.state == "error":
                q_err = "Channel not initialized" if queue_info is None else queue_info.error
                queue_status, healthy = f"error: {q_err}", False
                error_messages.append(f"Queue check failed: {q_err}")
            else:
                queue_status = "accessible"
        elif not consumer_queue and self._connection_manager._connected:
            healthy = False
            error_messages.append("Consumer queue not initialized")

        dlq_status: str | None = "disabled"
        if self._config.enable_dlq:
            if channel and not channel.is_closed:
                dlq_info = await self._topology.queue_health(self._config.dlq_queue_name)
                if dlq_info is None or dlq_info.state == "error":
                    dlq_err = "Channel not initialized" if dlq_info is None else dlq_info.error
                    dlq_status = f"error: {dlq_err}"
                    self._logger.warning(f"DLQ health check failed: {dlq_err}", extra={"dlq_queue": self._config.dlq_queue_name})  # fmt: skip
                else:
                    dlq_status = "accessible"
            else:
                dlq_status = "inaccessible"

        cfg, s = self._config, self._stats
        details: dict[str, Any] = {
            "exchange": cfg.exchange_name,
            "queue": cfg.queue_name,
            "consumer_group": cfg.consumer_group,
            "consuming": self._consumer._consuming,
            "dlq_enabled": cfg.enable_dlq,
        }
        if cfg.enable_dlq:
            details["dlq_queue"] = cfg.dlq_queue_name
        details["stats"] = {
            "events_published": s.events_published,
            "events_consumed": s.events_consumed,
            "events_processed_success": s.events_processed_success,
            "events_processed_failed": s.events_processed_failed,
            "messages_sent_to_dlq": s.messages_sent_to_dlq,
            "reconnections": s.reconnections,
        }

        error = "; ".join(error_messages) if error_messages else None
        extra = {"healthy": healthy, "connection_status": conn_status, "channel_status": chan_status, "queue_status": queue_status, "dlq_status": dlq_status}  # fmt: skip
        self._logger.debug(f"Health check completed: healthy={healthy}", extra=extra)
        return HealthCheckResult(
            healthy=healthy,
            connection_status=conn_status,
            channel_status=chan_status,
            queue_status=queue_status,
            dlq_status=dlq_status,
            error=error,
            details=details,
        )
