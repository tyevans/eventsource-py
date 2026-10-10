"""Integration tests for metrics and filtering statistics."""

from __future__ import annotations

import asyncio
from uuid import uuid4

import pytest

from eventsource.application.subscriptions.filtering import EventFilter
from eventsource.application.subscriptions.metrics import (
    OTEL_METRICS_AVAILABLE,
    SubscriptionMetrics,
    clear_metrics_registry,
)
from tests.integration.subscriptions.conftest import (
    SubTestOrderCancelled,
    SubTestOrderCreated,
    SubTestOrderShipped,
    populate_event_store_with_types,
)

pytestmark = pytest.mark.asyncio


class TestOpenTelemetryMetrics:
    """Tests for OpenTelemetry metrics recording."""

    def setup_method(self):
        """Clear metrics registry before each test."""
        clear_metrics_registry()

    async def test_metrics_graceful_degradation(self):
        """Test metrics work gracefully when OTel not configured."""
        metrics = SubscriptionMetrics(
            subscription_name="TestMetrics",
            enable_metrics=True,
        )

        metrics.record_event_processed("TestEvent", 10.5)
        metrics.record_event_failed("TestEvent", "ValueError")
        metrics.record_lag(100)
        metrics.record_state("live")

        snapshot = metrics.get_snapshot()
        assert snapshot.events_processed == 1
        assert snapshot.events_failed == 1
        assert snapshot.current_lag == 100
        assert snapshot.current_state_name == "live"

    async def test_metrics_disabled_gracefully(self):
        """Test metrics work when explicitly disabled."""
        metrics = SubscriptionMetrics(
            subscription_name="DisabledMetrics",
            enable_metrics=False,
        )

        metrics.record_event_processed("TestEvent", 10.5)
        metrics.record_event_failed("TestEvent", "ValueError")

        assert metrics.metrics_enabled is False

    async def test_metrics_time_processing_context_manager(self):
        """Test time_processing context manager."""
        metrics = SubscriptionMetrics(
            subscription_name="TimingMetrics",
            enable_metrics=True,
        )

        with metrics.time_processing() as timer:
            await asyncio.sleep(0.05)

        assert timer.duration_ms >= 50

    async def test_metrics_snapshot_accumulation(self):
        """Test that metrics snapshot accumulates correctly."""
        metrics = SubscriptionMetrics(
            subscription_name="AccumulationMetrics",
            enable_metrics=True,
        )

        for i in range(10):
            metrics.record_event_processed(f"Event{i}", float(i) * 10)

        for i in range(3):
            metrics.record_event_failed(f"FailEvent{i}", "Error")

        snapshot = metrics.get_snapshot()
        assert snapshot.events_processed == 10
        assert snapshot.events_failed == 3
        assert snapshot.total_processing_time_ms > 0

    async def test_metrics_state_mapping(self):
        """Test that state names map to correct numeric values."""
        metrics = SubscriptionMetrics(
            subscription_name="StateMappingMetrics",
            enable_metrics=True,
        )

        metrics.record_state("live")
        assert metrics.current_state == "live"

        metrics.record_state("catching_up")
        assert metrics.current_state == "catching_up"

        metrics.record_state("paused")
        assert metrics.current_state == "paused"

        metrics.record_state("error")
        assert metrics.current_state == "error"

    @pytest.mark.skipif(not OTEL_METRICS_AVAILABLE, reason="OpenTelemetry not installed")
    async def test_metrics_with_otel_available(self):
        """Test metrics work correctly when OTel is available."""
        clear_metrics_registry()

        metrics = SubscriptionMetrics(
            subscription_name="OTelMetrics",
            enable_metrics=True,
        )

        assert metrics.metrics_enabled is True

        metrics.record_event_processed("TestEvent", 15.5)
        metrics.record_event_failed("FailEvent", "TestError")
        metrics.record_lag(50)
        metrics.record_state("live")

        snapshot = metrics.get_snapshot()
        assert snapshot.events_processed == 1
        assert snapshot.events_failed == 1


class TestFilteringStatistics:
    """Tests for event filter statistics tracking."""

    async def test_filter_stats_tracked_correctly(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
    ):
        """Test that filter statistics are tracked correctly."""
        await populate_event_store_with_types(
            in_memory_event_store,
            created_count=20,
            shipped_count=10,
            cancelled_count=5,
        )

        filter = EventFilter(event_types=(SubTestOrderCreated,))

        created_events = [SubTestOrderCreated(aggregate_id=uuid4()) for _ in range(20)]
        shipped_events = [SubTestOrderShipped(aggregate_id=uuid4()) for _ in range(10)]
        cancelled_events = [SubTestOrderCancelled(aggregate_id=uuid4()) for _ in range(5)]

        for event in created_events + shipped_events + cancelled_events:
            filter.matches(event)

        stats = filter.stats
        assert stats.events_evaluated == 35
        assert stats.events_matched == 20
        assert stats.events_skipped == 15
        assert stats.match_rate == pytest.approx(20 / 35, rel=0.01)

    async def test_filter_stats_reset(self):
        """Test that filter statistics can be reset."""
        filter = EventFilter(event_types=(SubTestOrderCreated,))

        for _ in range(10):
            event = SubTestOrderCreated(aggregate_id=uuid4())
            filter.matches(event)

        assert filter.stats.events_evaluated == 10

        filter.reset_stats()

        assert filter.stats.events_evaluated == 0
        assert filter.stats.events_matched == 0
        assert filter.stats.events_skipped == 0
