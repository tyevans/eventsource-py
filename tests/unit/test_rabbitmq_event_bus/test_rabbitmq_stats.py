"""Unit tests extracted for test_rabbitmq_stats.py."""

from __future__ import annotations

from dataclasses import is_dataclass
from datetime import UTC, datetime

from eventsource.adapters.rabbitmq import RabbitMQEventBus, RabbitMQEventBusStats

_COUNTER_FIELDS = (
    "events_published",
    "events_consumed",
    "events_processed_success",
    "events_processed_failed",
    "messages_sent_to_dlq",
    "handler_errors",
    "reconnections",
    "publish_confirms",
    "publish_returns",
)


def _counter_tuple(stats: RabbitMQEventBusStats) -> tuple[int, ...]:
    return tuple(getattr(stats, f) for f in _COUNTER_FIELDS)


class TestRabbitMQEventBusStats:
    def test_default_values(self) -> None:
        assert _counter_tuple(RabbitMQEventBusStats()) == (0,) * 9

    def test_field_modification(self) -> None:
        stats = RabbitMQEventBusStats()
        expected = (100, 95, 90, 5, 2, 7, 3, 98, 2)
        for field, val in zip(_COUNTER_FIELDS, expected, strict=True):
            setattr(stats, field, val)
        assert _counter_tuple(stats) == expected

    def test_dataclass_equality(self) -> None:
        assert RabbitMQEventBusStats(events_published=10) == RabbitMQEventBusStats(
            events_published=10
        )

    def test_dataclass_inequality(self) -> None:
        assert RabbitMQEventBusStats(events_published=10) != RabbitMQEventBusStats(
            events_published=20
        )

    def test_partial_initialization(self) -> None:
        stats = RabbitMQEventBusStats(events_published=50, events_consumed=45)
        assert _counter_tuple(stats) == (50, 45, 0, 0, 0, 0, 0, 0, 0)

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
        assert _counter_tuple(stats) == (1000, 950, 900, 50, 10, 55, 5, 980, 20)

    def test_is_dataclass(self) -> None:
        assert is_dataclass(RabbitMQEventBusStats) and is_dataclass(RabbitMQEventBusStats())

    def test_increment_pattern(self) -> None:
        stats = RabbitMQEventBusStats()
        stats.events_published += 1
        stats.events_published += 1
        stats.events_consumed += 1
        stats.events_processed_success += 1
        assert (stats.events_published, stats.events_consumed, stats.events_processed_success) == (
            2,
            1,
            1,
        )

    def test_rabbitmq_specific_fields(self) -> None:
        stats = RabbitMQEventBusStats()
        stats.publish_confirms, stats.publish_returns = 100, 5
        assert (stats.publish_confirms, stats.publish_returns) == (100, 5)
        assert hasattr(stats, "publish_confirms") and hasattr(stats, "publish_returns")

    def test_common_fields_with_redis(self) -> None:
        stats = RabbitMQEventBusStats()
        for field in _COUNTER_FIELDS[:7]:
            assert hasattr(stats, field), f"Missing common field: {field}"

    def test_large_values(self) -> None:
        stats = RabbitMQEventBusStats()
        large_value = 10**12
        stats.events_published = stats.events_consumed = large_value
        assert (stats.events_published, stats.events_consumed) == (large_value, large_value)

    def test_repr(self) -> None:
        repr_str = repr(RabbitMQEventBusStats(events_published=10, events_consumed=8))
        assert all(
            s in repr_str
            for s in ("RabbitMQEventBusStats", "events_published=10", "events_consumed=8")
        )


class TestRabbitMQStatsResetBehavior:
    def test_stats_are_not_shared_between_instances(self) -> None:
        bus1, bus2 = RabbitMQEventBus(), RabbitMQEventBus()
        bus1.stats.events_published = 100
        assert bus2.stats.events_published == 0 and bus1.stats is not bus2.stats


class TestStatisticsTimingFields:
    def test_timing_fields_default_to_none(self) -> None:
        s = RabbitMQEventBusStats()
        assert (s.last_publish_at, s.last_consume_at, s.last_error_at, s.connected_at) == (
            None,
            None,
            None,
            None,
        )

    def test_timing_fields_can_be_set(self) -> None:
        now = datetime.now(UTC)
        s = RabbitMQEventBusStats(
            last_publish_at=now, last_consume_at=now, last_error_at=now, connected_at=now
        )
        assert (s.last_publish_at, s.last_consume_at, s.last_error_at, s.connected_at) == (
            now,
            now,
            now,
            now,
        )

    def test_timing_fields_independent(self) -> None:
        t1 = datetime(2024, 1, 1, 10, 0, 0, tzinfo=UTC)
        t2 = datetime(2024, 1, 1, 11, 0, 0, tzinfo=UTC)
        s = RabbitMQEventBusStats(last_publish_at=t1, last_consume_at=t2)
        assert (s.last_publish_at, s.last_consume_at, s.last_error_at, s.connected_at) == (
            t1,
            t2,
            None,
            None,
        )


class TestGetStatsMethod:
    def test_get_stats_returns_stats_object(self) -> None:
        assert isinstance(RabbitMQEventBus().get_stats(), RabbitMQEventBusStats)

    def test_get_stats_returns_same_as_property(self) -> None:
        bus = RabbitMQEventBus()
        assert bus.get_stats() is bus.stats

    def test_get_stats_reflects_modifications(self) -> None:
        bus = RabbitMQEventBus()
        bus._stats.events_published = 42
        assert bus.get_stats().events_published == 42
