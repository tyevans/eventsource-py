"""Kafka connection lifecycle management.

Holds the producer/consumer connection state for ``KafkaEventBus``:
connect/disconnect/cleanup, consumer reconnection after a consume-loop
error, security config for ad-hoc consumers (e.g. DLQ inspection), and
reconnection/rebalance metrics recording. Extracted from ``bus.py`` so the
facade can delegate connection concerns to a single collaborator instead of
managing producer/consumer state directly.

``KafkaRebalanceListener`` also lives here since its sole purpose is to
coordinate offset commits and rebalance-metric recording with the connection
manager during a Kafka consumer group rebalance.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

# Optional aiokafka import - fail gracefully if not installed
try:
    from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, TopicPartition
    from aiokafka.abc import ConsumerRebalanceListener as _ConsumerRebalanceListener
    from aiokafka.errors import IllegalStateError

    KAFKA_AVAILABLE = True
except ImportError:
    KAFKA_AVAILABLE = False
    AIOKafkaProducer = None
    AIOKafkaConsumer = None
    TopicPartition = None
    _ConsumerRebalanceListener = object
    IllegalStateError = Exception

if TYPE_CHECKING:
    from eventsource.adapters.kafka.config import KafkaEventBusConfig
    from eventsource.adapters.kafka.metrics import KafkaEventBusMetrics
    from eventsource.adapters.kafka.models import KafkaEventBusStats

# Pinned explicitly: __name__ is "eventsource.bus.kafka.connection" after the
# package move, but the public logger name must stay "eventsource.bus.kafka".
logger = logging.getLogger("eventsource.bus.kafka")


class KafkaConnectionManager:
    """Owns the Kafka producer/consumer connection lifecycle.

    Attributes:
        config: Kafka configuration used to build producer/consumer clients.
        stats: Shared statistics object updated on reconnection/rebalance.
        metrics: Shared metrics container updated on reconnection/rebalance.
    """

    def __init__(
        self,
        config: KafkaEventBusConfig,
        stats: KafkaEventBusStats,
        metrics: KafkaEventBusMetrics | None,
    ) -> None:
        """Initialize the connection manager.

        Args:
            config: Configuration for Kafka connection.
            stats: Shared statistics object (mutated in place).
            metrics: Shared metrics container, or None if metrics disabled.
        """
        self._config = config
        self._stats = stats
        self._metrics = metrics

        self._producer: AIOKafkaProducer | None = None
        self._consumer: AIOKafkaConsumer | None = None
        self._rebalance_listener: KafkaRebalanceListener | None = None
        self._connected = False

    # =========================================================================
    # Properties
    # =========================================================================

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
        """Replace the metrics container (used by tests wiring a test meter)."""
        self._metrics = value

    def require_producer(self) -> AIOKafkaProducer:
        """Get the active producer, raising if not connected.

        Returns:
            The active AIOKafkaProducer.

        Raises:
            RuntimeError: If not connected to Kafka.
        """
        if not self._producer:
            raise RuntimeError("Not connected to Kafka")
        return self._producer

    # =========================================================================
    # Connection Lifecycle Methods
    # =========================================================================

    async def connect(self) -> None:
        """Connect to Kafka cluster.

        Creates and starts the producer and consumer clients. The consumer
        subscribes to the configured topic.

        Raises:
            KafkaError: If connection to Kafka fails.
        """
        if self._connected:
            logger.warning("KafkaEventBus already connected")
            return

        logger.info(
            "Connecting to Kafka",
            extra=self._config.get_sanitized_config(),
        )

        try:
            # Create and start producer
            self._producer = AIOKafkaProducer(**self._config.get_producer_config())
            await self._producer.start()

            # Create and start consumer
            # Note: We pass the topic to the constructor for proper metadata loading.
            # aiokafka handles consumer group rebalancing internally; we track
            # rebalance events through the record_rebalance() method which can be
            # called externally or during reconnection.
            self._consumer = AIOKafkaConsumer(
                self._config.topic_name,
                **self._config.get_consumer_config(),
            )
            await self._consumer.start()

            # Initialize rebalance listener (available for future use or manual tracking)
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
            # Increment connection_errors counter
            if self._metrics:
                self._metrics.connection_errors.add(
                    1,
                    attributes={
                        "error.type": type(e).__name__,
                    },
                )

            logger.error(
                "Failed to connect to Kafka",
                extra={"error": str(e)},
                exc_info=True,
            )
            # Clean up partial connection
            await self.cleanup()
            raise

    async def disconnect(self) -> None:
        """Disconnect from Kafka cluster.

        Closes producer/consumer connections. Safe to call multiple times.

        Note: Stopping an active consume loop is the caller's responsibility
        (the facade stops consuming before delegating here).
        """
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
        """Attempt to reconnect the consumer after an error.

        Stops the current consumer and creates a new one with the same
        configuration and topic subscriptions.
        """
        logger.debug("Reconnecting consumer")

        # Stop the current consumer if it exists
        if self._consumer:
            try:
                await self._consumer.stop()
            except Exception as e:
                logger.warning(f"Error stopping consumer during reconnect: {e}")

        # Create and start new consumer with topic
        self._consumer = AIOKafkaConsumer(
            self._config.topic_name,
            **self._config.get_consumer_config(),
        )
        await self._consumer.start()

        logger.info("Consumer reconnected successfully")

    def get_security_config(self) -> dict[str, Any]:
        """Get security configuration for additional consumers.

        Creates a dictionary of security settings suitable for creating
        additional Kafka consumers (e.g., for DLQ inspection).

        Returns:
            Dictionary of security settings.
        """
        config: dict[str, Any] = {
            "security_protocol": self._config.security_protocol,
        }

        if self._config.sasl_mechanism:
            config["sasl_mechanism"] = self._config.sasl_mechanism
        if self._config.sasl_username:
            config["sasl_plain_username"] = self._config.sasl_username
        if self._config.sasl_password:
            config["sasl_plain_password"] = self._config.sasl_password
        if self._config.ssl_cafile:
            config["ssl_cafile"] = self._config.ssl_cafile
        if self._config.ssl_certfile:
            config["ssl_certfile"] = self._config.ssl_certfile
        if self._config.ssl_keyfile:
            config["ssl_keyfile"] = self._config.ssl_keyfile

        return config

    # =========================================================================
    # Metrics Recording
    # =========================================================================

    def record_reconnection(self) -> None:
        """Record a reconnection event for metrics.

        Call this method when a reconnection to Kafka occurs. Updates both
        the internal stats counter and the OpenTelemetry metrics counter.

        This method is safe to call even if metrics are disabled.
        """
        self._stats.reconnections += 1

        if self._metrics:
            self._metrics.reconnections.add(1)

        logger.debug("Reconnection recorded")

    def record_rebalance(self) -> None:
        """Record a consumer rebalance event for metrics.

        Call this method when a consumer group rebalance occurs. Updates both
        the internal stats counter and the OpenTelemetry metrics counter.

        This method is safe to call even if metrics are disabled.
        """
        self._stats.rebalance_count += 1

        if self._metrics:
            self._metrics.rebalances.add(
                1,
                attributes={
                    "messaging.kafka.consumer_group": self._config.consumer_group,
                },
            )

        logger.debug(
            "Rebalance recorded",
            extra={"consumer_group": self._config.consumer_group},
        )


class KafkaRebalanceListener(_ConsumerRebalanceListener):  # type: ignore[misc]
    """Consumer rebalance listener for handling partition assignment changes.

    This listener is called during consumer group rebalances to ensure proper
    offset management and prevent duplicate message processing during scaling
    events.

    The listener commits offsets for revoked partitions before they are
    reassigned to other consumers, ensuring at-least-once delivery guarantees
    are maintained during rebalances.

    Attributes:
        _manager: Reference to the KafkaConnectionManager for offset commits
            and metrics recording.
    """

    def __init__(self, manager: KafkaConnectionManager) -> None:
        """Initialize the rebalance listener.

        Args:
            manager: The KafkaConnectionManager instance to coordinate with.
        """
        self._manager = manager

    async def on_partitions_revoked(
        self,
        revoked: set[TopicPartition],
    ) -> None:
        """Called when partitions are being revoked from this consumer.

        This method commits offsets for all revoked partitions before they
        are assigned to other consumers. This ensures that:
        1. Messages processed but not yet committed are properly committed
        2. The new consumer starts from the correct offset
        3. No messages are processed twice due to rebalance

        Args:
            revoked: Set of TopicPartition objects being revoked.
        """
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

        # Commit offsets before partitions are revoked
        if self._manager._consumer:
            try:
                await self._manager._consumer.commit()
                logger.debug("Offsets committed before partition revocation")
            except IllegalStateError:
                # No partitions currently assigned - nothing to commit
                # This is expected during certain rebalance scenarios
                logger.debug("No partitions to commit during rebalance")
            except Exception as e:
                logger.warning(
                    "Failed to commit offsets during rebalance",
                    extra={"error": str(e)},
                )

        # Record the rebalance event
        self._manager.record_rebalance()

    async def on_partitions_assigned(
        self,
        assigned: set[TopicPartition],
    ) -> None:
        """Called when new partitions are assigned to this consumer.

        This method is called after partitions have been assigned and can
        be used for any initialization needed for the new partitions.

        Args:
            assigned: Set of TopicPartition objects newly assigned.
        """
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
