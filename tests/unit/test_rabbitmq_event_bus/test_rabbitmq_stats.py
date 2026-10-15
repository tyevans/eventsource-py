"""Unit tests extracted for test_rabbitmq_stats.py."""

from __future__ import annotations

from dataclasses import is_dataclass
from datetime import UTC, datetime

from eventsource.adapters.rabbitmq import RabbitMQEventBus, RabbitMQEventBusStats


class TestRabbitMQEventBusStats:
    """Tests for RabbitMQEventBusStats dataclass."""

    def test_default_values(self) -> None:
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
        stats1 = RabbitMQEventBusStats(events_published=10)
        stats2 = RabbitMQEventBusStats(events_published=10)
        assert stats1 == stats2

    def test_dataclass_inequality(self) -> None:
        stats1 = RabbitMQEventBusStats(events_published=10)
        stats2 = RabbitMQEventBusStats(events_published=20)
        assert stats1 != stats2

    def test_partial_initialization(self) -> None:
        stats = RabbitMQEventBusStats(events_published=50, events_consumed=45)
        assert stats.events_published == 50
        assert stats.events_consumed == 45
        assert stats.events_processed_success == 0
        assert stats.events_processed_failed == 0
        assert stats.messages_sent_to_dlq == 0
        assert stats.handler_errors == 0
        assert stats.reconnections == 0
        assert stats.publish_confirms == 0
        assert stats.publish_returns == 0

    def test_full_initialization(self) -> None:
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
        assert is_dataclass(RabbitMQEventBusStats)
        assert is_dataclass(RabbitMQEventBusStats())

    def test_increment_pattern(self) -> None:
        stats = RabbitMQEventBusStats()
        stats.events_published += 1
        stats.events_published += 1
        stats.events_consumed += 1
        stats.events_processed_success += 1
        assert stats.events_published == 2
        assert stats.events_consumed == 1
        assert stats.events_processed_success == 1

    def test_rabbitmq_specific_fields(self) -> None:
        stats = RabbitMQEventBusStats()
        stats.publish_confirms = 100
        stats.publish_returns = 5
        assert stats.publish_confirms == 100
        assert stats.publish_returns == 5
        assert hasattr(stats, "publish_confirms")
        assert hasattr(stats, "publish_returns")

    def test_common_fields_with_redis(self) -> None:
        stats = RabbitMQEventBusStats()
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
        stats = RabbitMQEventBusStats()
        large_value = 10**12
        stats.events_published = large_value
        stats.events_consumed = large_value
        assert stats.events_published == large_value
        assert stats.events_consumed == large_value

    def test_repr(self) -> None:
        stats = RabbitMQEventBusStats(events_published=10, events_consumed=8)
        repr_str = repr(stats)
        assert "RabbitMQEventBusStats" in repr_str
        assert "events_published=10" in repr_str
        assert "events_consumed=8" in repr_str


class TestRabbitMQStatsResetBehavior:
    """Tests for stats behavior and edge cases."""

    def test_stats_are_not_shared_between_instances(self) -> None:
        bus1 = RabbitMQEventBus()
        bus2 = RabbitMQEventBus()
        bus1.stats.events_published = 100
        assert bus2.stats.events_published == 0
        assert bus1.stats is not bus2.stats


class TestStatisticsTimingFields:
    """Tests for timing fields in RabbitMQEventBusStats."""

    def test_timing_fields_default_to_none(self) -> None:
        stats = RabbitMQEventBusStats()
        assert stats.last_publish_at is None
        assert stats.last_consume_at is None
        assert stats.last_error_at is None
        assert stats.connected_at is None

    def test_timing_fields_can_be_set(self) -> None:
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
        publish_time = datetime(2024, 1, 1, 10, 0, 0, tzinfo=UTC)
        consume_time = datetime(2024, 1, 1, 11, 0, 0, tzinfo=UTC)
        stats = RabbitMQEventBusStats(last_publish_at=publish_time, last_consume_at=consume_time)
        assert stats.last_publish_at == publish_time
        assert stats.last_consume_at == consume_time
        assert stats.last_error_at is None
        assert stats.connected_at is None


class TestGetStatsMethod:
    """Tests for RabbitMQEventBus.get_stats() method."""

    def test_get_stats_returns_stats_object(self) -> None:
        bus = RabbitMQEventBus()
        assert isinstance(bus.get_stats(), RabbitMQEventBusStats)

    def test_get_stats_returns_same_as_property(self) -> None:
        bus = RabbitMQEventBus()
        assert bus.get_stats() is bus.stats

    def test_get_stats_reflects_modifications(self) -> None:
        bus = RabbitMQEventBus()
        bus._stats.events_published = 42
        assert bus.get_stats().events_published == 42
