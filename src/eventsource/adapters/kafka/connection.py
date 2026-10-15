"""Kafka connection lifecycle management and consumer rebalance listener."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

try:
    from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
    from aiokafka.abc import ConsumerRebalanceListener as _ConsumerRebalanceListener
    from aiokafka.errors import IllegalStateError
except ImportError:
    AIOKafkaProducer = None
    AIOKafkaConsumer = None
    TopicPartition = None
    _ConsumerRebalanceListener = object
    IllegalStateError = Exception

if TYPE_CHECKING:
    from eventsource.adapters.kafka.config import KafkaEventBusConfig
    from eventsource.adapters.kafka.metrics import KafkaEventBusMetrics
    from eventsource.adapters.kafka.models import KafkaEventBusStats

logger = logging.getLogger("eventsource.bus.kafka")


class KafkaConnectionManager:
    """Owns the Kafka producer/consumer connection lifecycle."""

    def __init__(
        self,
        config: KafkaEventBusConfig,
        stats: KafkaEventBusStats,
        metrics: KafkaEventBusMetrics | None,
    ) -> None:
        self._config = config
        self._stats = stats
        self._metrics = metrics

        self._producer: AIOKafkaProducer | None = None
        self._consumer: AIOKafkaConsumer | None = None
        self._rebalance_listener: KafkaRebalanceListener | None = None
        self._connected = False

    @property
    def producer(self) -> AIOKafkaProducer | None:
        """Get the active producer, or None if not connected."""
        return self._producer

    @property
    def consumer(self) -> AIOKafkaConsumer | None:
        """Get the active consumer, or None if not connected."""
        return self._consumer

    @property
    def is_connected(self) -> bool:
        """Check if connected to Kafka."""
        return self._connected

    @property
    def metrics(self) -> KafkaEventBusMetrics | None:
        """Get the current metrics container."""
        return self._metrics

    @metrics.setter
    def metrics(self, value: KafkaEventBusMetrics | None) -> None:
        """Replace the metrics container."""
        self._metrics = value

    def require_producer(self) -> AIOKafkaProducer:
        """Get the active producer, raising RuntimeError if not connected."""
        if not self._producer:
            raise RuntimeError("Not connected to Kafka")
        return self._producer

    async def connect(self) -> None:
        """Connect to Kafka cluster, starting producer and consumer clients."""
        if self._connected:
            logger.warning("KafkaEventBus already connected")
            return

        logger.info("Connecting to Kafka", extra=self._config.get_sanitized_config())

        try:
            self._producer = AIOKafkaProducer(**self._config.get_producer_config())
            await self._producer.start()

            self._consumer = AIOKafkaConsumer(
                self._config.topic_name,
                **self._config.get_consumer_config(),
            )
            await self._consumer.start()

            self._rebalance_listener = KafkaRebalanceListener(self)
            self._connected = True
            self._stats.connected_at = datetime.now(UTC)

            logger.info(
                "Connected to Kafka",
                extra={
                    "topic": self._config.topic_name,
                    "consumer_group": self._config.consumer_group,
                },
            )
        except Exception as e:
            if self._metrics:
                self._metrics.connection_errors.add(
                    1,
                    attributes={"error.type": type(e).__name__},
                )
            logger.error("Failed to connect to Kafka", extra={"error": str(e)}, exc_info=True)
            await self.cleanup()
            raise

    async def disconnect(self) -> None:
        """Disconnect from Kafka cluster. Safe to call multiple times."""
        if not self._connected:
            logger.debug("KafkaEventBus not connected, nothing to disconnect")
            return

        logger.info("Disconnecting from Kafka")
        await self.cleanup()
        self._connected = False
        logger.info("Disconnected from Kafka")

    async def cleanup(self) -> None:
        """Clean up producer and consumer connections."""
        if self._producer:
            try:
                await self._producer.stop()
            except Exception as e:
                logger.warning(f"Error stopping producer: {e}")
            self._producer = None

        if self._consumer:
            try:
                await self._consumer.stop()
            except Exception as e:
                logger.warning(f"Error stopping consumer: {e}")
            self._consumer = None

    async def reconnect_consumer(self) -> None:
        """Stop the current consumer and start a new one with the same subscription."""
        logger.debug("Reconnecting consumer")

        if self._consumer:
            try:
                await self._consumer.stop()
            except Exception as e:
                logger.warning(f"Error stopping consumer during reconnect: {e}")

        self._consumer = AIOKafkaConsumer(
            self._config.topic_name,
            **self._config.get_consumer_config(),
        )
        await self._consumer.start()
        logger.info("Consumer reconnected successfully")

    def get_security_config(self) -> dict[str, Any]:
        """Get security configuration for additional consumers."""
        return self._config.get_security_config()

    def record_reconnection(self) -> None:
        """Record a reconnection event in stats and metrics."""
        self._stats.reconnections += 1
        if self._metrics:
            self._metrics.reconnections.add(1)
        logger.debug("Reconnection recorded")

    def record_rebalance(self) -> None:
        """Record a consumer rebalance event in stats and metrics."""
        self._stats.rebalance_count += 1
        if self._metrics:
            self._metrics.rebalances.add(
                1,
                attributes={"messaging.kafka.consumer_group": self._config.consumer_group},
            )
        logger.debug(
            "Rebalance recorded",
            extra={"consumer_group": self._config.consumer_group},
        )


class KafkaRebalanceListener(_ConsumerRebalanceListener):  # type: ignore[misc]
    """Commits offsets for revoked partitions and logs assignment changes during rebalances."""

    def __init__(self, manager: KafkaConnectionManager) -> None:
        self._manager = manager

    async def on_partitions_revoked(self, revoked: set[TopicPartition]) -> None:
        """Commit offsets and record metrics when partitions are revoked."""
        if not revoked:
            return

        logger.info(
            "Partitions being revoked, committing offsets",
            extra={
                "revoked_partitions": [
                    {"topic": tp.topic, "partition": tp.partition} for tp in revoked
                ],
                "consumer_group": self._manager._config.consumer_group,
            },
        )

        if self._manager._consumer:
            try:
                await self._manager._consumer.commit()
                logger.debug("Offsets committed before partition revocation")
            except IllegalStateError:
                logger.debug("No partitions to commit during rebalance")
            except Exception as e:
                logger.warning(
                    "Failed to commit offsets during rebalance",
                    extra={"error": str(e)},
                )

        self._manager.record_rebalance()

    async def on_partitions_assigned(self, assigned: set[TopicPartition]) -> None:
        """Log newly assigned partitions after a rebalance."""
        if not assigned:
            return

        logger.info(
            "New partitions assigned",
            extra={
                "assigned_partitions": [
                    {"topic": tp.topic, "partition": tp.partition} for tp in assigned
                ],
                "consumer_group": self._manager._config.consumer_group,
            },
        )
