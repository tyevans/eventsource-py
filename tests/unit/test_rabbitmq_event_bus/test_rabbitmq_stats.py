"""Unit tests extracted for test_rabbitmq_stats.py."""

from __future__ import annotations

from datetime import UTC, datetime

from eventsource.adapters.rabbitmq import (
    RabbitMQEventBus,
    RabbitMQEventBusStats,
)


class TestRabbitMQEventBusStats:
    """Tests for RabbitMQEventBusStats dataclass."""

    def test_default_values(self) -> None:
        """Test that all fields default to zero."""
        stats = RabbitMQEventBusStats()

        assert stats.events_published == 0
        assert stats.events_consumed == 0
        assert stats.events_processed_success == 0
        assert stats.events_processed_failed == 0
        assert stats.messages_sent_to_dlq == 0
        assert stats.handler_errors == 0
        assert stats.reconnections == 0
        assert stats.publish_confirms == 0
        assert stats.publish_returns == 0

    def test_field_modification(self) -> None:
        """Test that fields can be modified after creation."""
        stats = RabbitMQEventBusStats()

        stats.events_published = 100
        stats.events_consumed = 95
        stats.events_processed_success = 90
        stats.events_processed_failed = 5
        stats.messages_sent_to_dlq = 2
        stats.handler_errors = 7
        stats.reconnections = 3
        stats.publish_confirms = 98
        stats.publish_returns = 2

        assert stats.events_published == 100
        assert stats.events_consumed == 95
        assert stats.events_processed_success == 90
        assert stats.events_processed_failed == 5
        assert stats.messages_sent_to_dlq == 2
        assert stats.handler_errors == 7
        assert stats.reconnections == 3
        assert stats.publish_confirms == 98
        assert stats.publish_returns == 2

    def test_dataclass_equality(self) -> None:
        """Test dataclass equality comparison."""
        stats1 = RabbitMQEventBusStats(events_published=10)
        stats2 = RabbitMQEventBusStats(events_published=10)

        assert stats1 == stats2

    def test_dataclass_inequality(self) -> None:
        """Test dataclass inequality comparison."""
        stats1 = RabbitMQEventBusStats(events_published=10)
        stats2 = RabbitMQEventBusStats(events_published=20)

        assert stats1 != stats2

    def test_partial_initialization(self) -> None:
        """Test creating stats with only some fields specified."""
        stats = RabbitMQEventBusStats(
            events_published=50,
            events_consumed=45,
        )

        assert stats.events_published == 50
        assert stats.events_consumed == 45
        # Other fields should be default
        assert stats.events_processed_success == 0
        assert stats.events_processed_failed == 0
        assert stats.messages_sent_to_dlq == 0
        assert stats.handler_errors == 0
        assert stats.reconnections == 0
        assert stats.publish_confirms == 0
        assert stats.publish_returns == 0

    def test_full_initialization(self) -> None:
        """Test creating stats with all fields specified."""
        stats = RabbitMQEventBusStats(
            events_published=1000,
            events_consumed=950,
            events_processed_success=900,
            events_processed_failed=50,
            messages_sent_to_dlq=10,
            handler_errors=55,
            reconnections=5,
            publish_confirms=980,
            publish_returns=20,
        )

        assert stats.events_published == 1000
        assert stats.events_consumed == 950
        assert stats.events_processed_success == 900
        assert stats.events_processed_failed == 50
        assert stats.messages_sent_to_dlq == 10
        assert stats.handler_errors == 55
        assert stats.reconnections == 5
        assert stats.publish_confirms == 980
        assert stats.publish_returns == 20

    def test_is_dataclass(self) -> None:
        """Test that stats is a proper dataclass."""
        from dataclasses import is_dataclass

        assert is_dataclass(RabbitMQEventBusStats)
        assert is_dataclass(RabbitMQEventBusStats())

    def test_increment_pattern(self) -> None:
        """Test that fields can be incremented (common usage pattern)."""
        stats = RabbitMQEventBusStats()

        # Simulate incrementing during operation
        stats.events_published += 1
        stats.events_published += 1
        stats.events_consumed += 1
        stats.events_processed_success += 1

        assert stats.events_published == 2
        assert stats.events_consumed == 1
        assert stats.events_processed_success == 1

    def test_rabbitmq_specific_fields(self) -> None:
        """Test RabbitMQ-specific fields that differ from Redis."""
        stats = RabbitMQEventBusStats()

        # RabbitMQ-specific: publish_confirms and publish_returns
        stats.publish_confirms = 100
        stats.publish_returns = 5

        assert stats.publish_confirms == 100
        assert stats.publish_returns == 5

        # Verify these fields exist (RabbitMQ-specific)
        assert hasattr(stats, "publish_confirms")
        assert hasattr(stats, "publish_returns")

    def test_common_fields_with_redis(self) -> None:
        """Test that common fields match those in RedisEventBusStats."""
        stats = RabbitMQEventBusStats()

        # These fields should exist in both Redis and RabbitMQ stats
        common_fields = [
            "events_published",
            "events_consumed",
            "events_processed_success",
            "events_processed_failed",
            "messages_sent_to_dlq",
            "handler_errors",
            "reconnections",
        ]

        for field in common_fields:
            assert hasattr(stats, field), f"Missing common field: {field}"

    def test_large_values(self) -> None:
        """Test that stats can handle large values."""
        stats = RabbitMQEventBusStats()

        large_value = 10**12  # 1 trillion
        stats.events_published = large_value
        stats.events_consumed = large_value

        assert stats.events_published == large_value
        assert stats.events_consumed == large_value

    def test_repr(self) -> None:
        """Test string representation of stats."""
        stats = RabbitMQEventBusStats(events_published=10, events_consumed=8)

        repr_str = repr(stats)
        assert "RabbitMQEventBusStats" in repr_str
        assert "events_published=10" in repr_str
        assert "events_consumed=8" in repr_str


class TestRabbitMQStatsResetBehavior:
    """Tests for stats behavior and edge cases."""

    def test_stats_are_not_shared_between_instances(self) -> None:
        """Test that each bus instance has its own stats object."""
        from eventsource.adapters.rabbitmq import RabbitMQEventBus

        bus1 = RabbitMQEventBus()
        bus2 = RabbitMQEventBus()

        bus1.stats.events_published = 100

        assert bus2.stats.events_published == 0
        assert bus1.stats is not bus2.stats


class TestStatisticsTimingFields:
    """Tests for timing fields in RabbitMQEventBusStats."""

    def test_timing_fields_default_to_none(self) -> None:
        """Test that timing fields default to None."""
        stats = RabbitMQEventBusStats()

        assert stats.last_publish_at is None
        assert stats.last_consume_at is None
        assert stats.last_error_at is None
        assert stats.connected_at is None

    def test_timing_fields_can_be_set(self) -> None:
        """Test that timing fields can be set to datetime values."""
        now = datetime.now(UTC)
        stats = RabbitMQEventBusStats(
            last_publish_at=now,
            last_consume_at=now,
            last_error_at=now,
            connected_at=now,
        )

        assert stats.last_publish_at == now
        assert stats.last_consume_at == now
        assert stats.last_error_at == now
        assert stats.connected_at == now

    def test_timing_fields_independent(self) -> None:
        """Test that timing fields can be set independently."""
        publish_time = datetime(2024, 1, 1, 10, 0, 0, tzinfo=UTC)
        consume_time = datetime(2024, 1, 1, 11, 0, 0, tzinfo=UTC)

        stats = RabbitMQEventBusStats(
            last_publish_at=publish_time,
            last_consume_at=consume_time,
        )

        assert stats.last_publish_at == publish_time
        assert stats.last_consume_at == consume_time
        assert stats.last_error_at is None
        assert stats.connected_at is None


class TestGetStatsMethod:
    """Tests for RabbitMQEventBus.get_stats() method."""

    def test_get_stats_returns_stats_object(self) -> None:
        """Test that get_stats() returns a RabbitMQEventBusStats object."""
        bus = RabbitMQEventBus()
        stats = bus.get_stats()

        assert isinstance(stats, RabbitMQEventBusStats)

    def test_get_stats_returns_same_as_property(self) -> None:
        """Test that get_stats() returns the same object as stats property."""
        bus = RabbitMQEventBus()

        assert bus.get_stats() is bus.stats

    def test_get_stats_reflects_modifications(self) -> None:
        """Test that get_stats() reflects modifications to stats."""
        bus = RabbitMQEventBus()
        bus._stats.events_published = 42

        stats = bus.get_stats()
        assert stats.events_published == 42
