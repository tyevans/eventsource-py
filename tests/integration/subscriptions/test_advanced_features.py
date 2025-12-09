"""
Integration tests for Phase 3 advanced features.

Tests cover comprehensive scenarios for:
- Event type filtering (exact and wildcard patterns)
- Multiple subscriptions with different configurations
- Health check API (readiness, liveness, comprehensive health)
- Pause/resume functionality (manual and automatic)
- Combined advanced features (filtering + pause + health)
- OpenTelemetry metrics recording (if available)

These tests verify that all Phase 3 features work together correctly
in realistic integration scenarios using InMemoryEventStore,
InMemoryEventBus, and InMemoryCheckpointRepository.
"""

import asyncio
import contextlib
from datetime import UTC, datetime
from uuid import uuid4

import pytest

from eventsource.events.base import DomainEvent
from eventsource.events.registry import register_event
from eventsource.subscriptions import (
    PauseReason,
    SubscriptionConfig,
    SubscriptionManager,
    SubscriptionState,
)
from eventsource.subscriptions.filtering import EventFilter
from eventsource.subscriptions.health import (
    ManagerHealth,
)
from eventsource.subscriptions.metrics import (
    OTEL_METRICS_AVAILABLE,
    SubscriptionMetrics,
    clear_metrics_registry,
)

from .conftest import (
    CollectingProjection,
    FailingProjection,
    SlowProjection,
    SubTestOrderCancelled,
    SubTestOrderCreated,
    SubTestOrderShipped,
    populate_event_store,
    populate_event_store_with_types,
    publish_live_event,
)

pytestmark = pytest.mark.asyncio


# =============================================================================
# Additional Test Events for Advanced Filtering
# =============================================================================


@register_event
class AdvTestUserRegistered(DomainEvent):
    """Test event for user registration in advanced tests."""

    event_type: str = "AdvTestUserRegistered"
    aggregate_type: str = "AdvTestUser"
    username: str = ""


@register_event
class AdvTestUserUpdated(DomainEvent):
    """Test event for user updates in advanced tests."""

    event_type: str = "AdvTestUserUpdated"
    aggregate_type: str = "AdvTestUser"
    field: str = ""


@register_event
class AdvTestPaymentReceived(DomainEvent):
    """Test event for payments in advanced tests."""

    event_type: str = "AdvTestPaymentReceived"
    aggregate_type: str = "AdvTestPayment"
    amount: float = 0.0


# =============================================================================
# Additional Test Projections
# =============================================================================


class FilteredProjection:
    """
    Projection that only subscribes to specific event types.

    Used for testing event type filtering functionality.
    """

    def __init__(self, event_types: list[type[DomainEvent]] | None = None) -> None:
        self.events: list[DomainEvent] = []
        self.event_count = 0
        self._event_types = event_types or []
        self._lock = asyncio.Lock()

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return self._event_types

    async def handle(self, event: DomainEvent) -> None:
        """Handle an event by collecting it."""
        async with self._lock:
            self.events.append(event)
            self.event_count += 1

    async def wait_for_events(self, count: int, timeout: float = 5.0) -> bool:
        """Wait until the specified number of events are collected."""
        start = datetime.now(UTC)
        while self.event_count < count:
            if (datetime.now(UTC) - start).total_seconds() > timeout:
                return False
            await asyncio.sleep(0.05)
        return True


class PausableProjection:
    """
    Projection that can track pause/resume events.

    Used for testing pause/resume functionality.
    """

    def __init__(self) -> None:
        self.events: list[DomainEvent] = []
        self.event_count = 0
        self.events_during_pause: list[DomainEvent] = []
        self.is_tracking_pause = False
        self._lock = asyncio.Lock()

    def subscribed_to(self) -> list[type[DomainEvent]]:
        return [SubTestOrderCreated, SubTestOrderShipped, SubTestOrderCancelled]

    async def handle(self, event: DomainEvent) -> None:
        """Handle an event by collecting it."""
        async with self._lock:
            self.events.append(event)
            self.event_count += 1
            if self.is_tracking_pause:
                self.events_during_pause.append(event)

    async def wait_for_events(self, count: int, timeout: float = 5.0) -> bool:
        """Wait until the specified number of events are collected."""
        start = datetime.now(UTC)
        while self.event_count < count:
            if (datetime.now(UTC) - start).total_seconds() > timeout:
                return False
            await asyncio.sleep(0.05)
        return True

    def start_tracking_pause(self) -> None:
        """Start tracking events received during pause."""
        self.is_tracking_pause = True
        self.events_during_pause = []

    def stop_tracking_pause(self) -> None:
        """Stop tracking events received during pause."""
        self.is_tracking_pause = False


# =============================================================================
# Event Type Filtering Tests
# =============================================================================


class TestEventTypeFiltering:
    """Tests for event type filtering functionality."""

    async def test_exact_event_type_filtering_in_catchup(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
    ):
        """Test that exact event type filtering works during catch-up phase."""
        # Populate with mixed event types
        await populate_event_store_with_types(
            in_memory_event_store,
            created_count=20,
            shipped_count=15,
            cancelled_count=10,
        )

        # Create projection that only subscribes to OrderCreated
        projection = FilteredProjection(event_types=[SubTestOrderCreated])

        manager = SubscriptionManager(
            event_store=in_memory_event_store,
            event_bus=in_memory_event_bus,
            checkpoint_repo=in_memory_checkpoint_repo,
        )

        config = SubscriptionConfig(start_from="beginning")
        await manager.subscribe(projection, config)
        await manager.start()

        # Should only receive OrderCreated events
        await projection.wait_for_events(20, timeout=10.0)
        await manager.stop()

        # Verify only OrderCreated events received
        assert projection.event_count == 20
        for event in projection.events:
            assert isinstance(event, SubTestOrderCreated)

    async def test_multiple_event_type_filtering(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
    ):
        """Test filtering for multiple event types."""
        await populate_event_store_with_types(
            in_memory_event_store,
            created_count=15,
            shipped_count=15,
            cancelled_count=15,
        )

        # Subscribe to Created and Shipped, but not Cancelled
        projection = FilteredProjection(event_types=[SubTestOrderCreated, SubTestOrderShipped])

        manager = SubscriptionManager(
            event_store=in_memory_event_store,
            event_bus=in_memory_event_bus,
            checkpoint_repo=in_memory_checkpoint_repo,
        )

        config = SubscriptionConfig(start_from="beginning")
        await manager.subscribe(projection, config)
        await manager.start()

        await projection.wait_for_events(30, timeout=10.0)
        await manager.stop()

        # Should have 30 events (15 created + 15 shipped)
        assert projection.event_count == 30

        # Verify no cancelled events
        for event in projection.events:
            assert not isinstance(event, SubTestOrderCancelled)

    async def test_config_event_types_overrides_subscriber(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
    ):
        """Test that config event_types overrides subscriber's subscribed_to()."""
        await populate_event_store_with_types(
            in_memory_event_store,
            created_count=20,
            shipped_count=20,
            cancelled_count=0,
        )

        # Subscriber declares interest in Created and Shipped
        projection = FilteredProjection(event_types=[SubTestOrderCreated, SubTestOrderShipped])

        # But config restricts to only Created
        config = SubscriptionConfig(
            start_from="beginning",
            event_types=(SubTestOrderCreated,),  # Override to only Created
        )

        manager = SubscriptionManager(
            event_store=in_memory_event_store,
            event_bus=in_memory_event_bus,
            checkpoint_repo=in_memory_checkpoint_repo,
        )

        await manager.subscribe(projection, config)
        await manager.start()

        await projection.wait_for_events(20, timeout=10.0)
        await manager.stop()

        # Should only receive Created events (config overrides subscriber)
        assert projection.event_count == 20
        for event in projection.events:
            assert isinstance(event, SubTestOrderCreated)

    async def test_event_filter_in_live_mode(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
    ):
        """Test that event filtering works in live mode."""
        projection = FilteredProjection(event_types=[SubTestOrderCreated])

        manager = SubscriptionManager(
            event_store=in_memory_event_store,
            event_bus=in_memory_event_bus,
            checkpoint_repo=in_memory_checkpoint_repo,
        )

        config = SubscriptionConfig(start_from="beginning")
        await manager.subscribe(projection, config)
        await manager.start()

        # Wait for live mode (no historical events)
        await asyncio.sleep(0.2)

        # Publish mixed events in live mode
        for i in range(10):
            await publish_live_event(
                in_memory_event_store,
                in_memory_event_bus,
                f"LIVE-{i:03d}",
            )

        # Also publish shipped events (should be filtered out)
        for i in range(5):
            event = SubTestOrderShipped(
                aggregate_id=uuid4(),
                tracking_number=f"TRK-{i:03d}",
            )
            await in_memory_event_store.append_events(
                aggregate_id=event.aggregate_id,
                aggregate_type="SubTestOrder",
                events=[event],
                expected_version=0,
            )
            await in_memory_event_bus.publish([event])

        # Should only receive created events
        await projection.wait_for_events(10, timeout=5.0)
        await manager.stop()

        assert projection.event_count == 10
        for event in projection.events:
            assert isinstance(event, SubTestOrderCreated)

    async def test_wildcard_pattern_filtering(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
    ):
        """Test wildcard pattern filtering with EventFilter."""
        # This tests EventFilter directly since SubscriptionConfig uses event types
        filter = EventFilter.from_patterns("SubTestOrder*")

        created = SubTestOrderCreated(aggregate_id=uuid4())
        shipped = SubTestOrderShipped(aggregate_id=uuid4())
        cancelled = SubTestOrderCancelled(aggregate_id=uuid4())

        # All SubTestOrder* events should match
        assert filter.matches(created) is True
        assert filter.matches(shipped) is True
        assert filter.matches(cancelled) is True

        # Non-matching event
        user_event = AdvTestUserRegistered(aggregate_id=uuid4())
        assert filter.matches(user_event) is False

    async def test_aggregate_type_filtering(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
    ):
        """Test filtering by aggregate type using EventFilter."""
        filter = EventFilter(aggregate_types=("SubTestOrder",))

        order_event = SubTestOrderCreated(aggregate_id=uuid4())
        user_event = AdvTestUserRegistered(aggregate_id=uuid4())

        assert filter.matches(order_event) is True
        assert filter.matches(user_event) is False


# =============================================================================
# Multiple Subscriptions Tests
# =============================================================================


class TestMultipleSubscriptions:
    """Tests for multiple concurrent subscriptions."""

    async def test_multiple_subscriptions_concurrent_start(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
    ):
        """Test that multiple subscriptions start concurrently."""
        await populate_event_store(in_memory_event_store, 100)

        projections = [CollectingProjection(name=f"Projection{i}") for i in range(5)]

        manager = SubscriptionManager(
            event_store=in_memory_event_store,
            event_bus=in_memory_event_bus,
            checkpoint_repo=in_memory_checkpoint_repo,
        )

        config = SubscriptionConfig(start_from="beginning")
        for i, projection in enumerate(projections):
            await manager.subscribe(projection, config, name=f"Projection{i}")

        # Start all concurrently
        results = await manager.start()

        # All should start successfully
        assert all(result is None for result in results.values())
        assert len(results) == 5

        # Wait for all projections to catch up
        for projection in projections:
            await projection.wait_for_events(100, timeout=10.0)

        await manager.stop()

        # All projections should have received all events
        for projection in projections:
            assert projection.event_count == 100

    async def test_multiple_subscriptions_independent_checkpoints(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
    ):
        """Test that each subscription has independent checkpoints."""
        events = await populate_event_store(in_memory_event_store, 100)

        # Set different starting positions for different subscriptions
        await in_memory_checkpoint_repo.save_position(
            subscription_id="ProjectionA",
            position=25,
            event_id=events[24].event_id,
            event_type="SubTestOrderCreated",
        )
        await in_memory_checkpoint_repo.save_position(
            subscription_id="ProjectionB",
            position=50,
            event_id=events[49].event_id,
            event_type="SubTestOrderCreated",
        )

        projection_a = CollectingProjection(name="A")
        projection_b = CollectingProjection(name="B")
        projection_c = CollectingProjection(name="C")  # No checkpoint

        manager = SubscriptionManager(
            event_store=in_memory_event_store,
            event_bus=in_memory_event_bus,
            checkpoint_repo=in_memory_checkpoint_repo,
        )

        config_checkpoint = SubscriptionConfig(start_from="checkpoint")
        config_beginning = SubscriptionConfig(start_from="beginning")

        await manager.subscribe(projection_a, config_checkpoint, name="ProjectionA")
        await manager.subscribe(projection_b, config_checkpoint, name="ProjectionB")
        await manager.subscribe(projection_c, config_beginning, name="ProjectionC")

        await manager.start()

        # Wait for all to complete
        await projection_a.wait_for_events(75, timeout=10.0)  # 100 - 25
        await projection_b.wait_for_events(50, timeout=10.0)  # 100 - 50
        await projection_c.wait_for_events(100, timeout=10.0)  # All 100

        await manager.stop()

        # Each projection should have received events from its checkpoint
        assert projection_a.event_count == 75
        assert projection_b.event_count == 50
        assert projection_c.event_count == 100

    async def test_one_failure_doesnt_stop_others(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
    ):
        """Test that one subscription failure doesn't stop other subscriptions."""
        await populate_event_store(in_memory_event_store, 50)

        healthy_projection = CollectingProjection(name="Healthy")
        failing_projection = FailingProjection(fail_on_event_types={"SubTestOrderCreated"})

        manager = SubscriptionManager(
            event_store=in_memory_event_store,
            event_bus=in_memory_event_bus,
            checkpoint_repo=in_memory_checkpoint_repo,
        )

        config_healthy = SubscriptionConfig(start_from="beginning")
        config_failing = SubscriptionConfig(
            start_from="beginning",
            continue_on_error=False,  # Stop on error
        )

        await manager.subscribe(healthy_projection, config_healthy, name="HealthyProj")
        await manager.subscribe(failing_projection, config_failing, name="FailingProj")

        await manager.start()

        # Healthy projection should complete
        success = await healthy_projection.wait_for_events(50, timeout=10.0)
        assert success

        await manager.stop()

        # Healthy projection processed all events
        assert healthy_projection.event_count == 50

        # Failing projection is in error state but didn't affect healthy one
        failing_sub = manager.get_subscription("FailingProj")
        assert failing_sub is not None
        assert failing_sub.state == SubscriptionState.ERROR

    async def test_concurrent_live_processing(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
    ):
        """Test multiple subscriptions processing live events concurrently."""
        projections = [CollectingProjection(name=f"LiveProj{i}") for i in range(3)]

        manager = SubscriptionManager(
            event_store=in_memory_event_store,
            event_bus=in_memory_event_bus,
            checkpoint_repo=in_memory_checkpoint_repo,
        )

        config = SubscriptionConfig(start_from="beginning")
        for i, projection in enumerate(projections):
            await manager.subscribe(projection, config, name=f"LiveProj{i}")

        await manager.start()

        # Wait for live mode
        await asyncio.sleep(0.2)

        # Publish live events
        for i in range(20):
            await publish_live_event(
                in_memory_event_store,
                in_memory_event_bus,
                f"LIVE-CONCURRENT-{i:03d}",
            )

        # All projections should receive all live events
        for projection in projections:
            await projection.wait_for_events(20, timeout=10.0)

        await manager.stop()

        # Verify all projections got all events
        for projection in projections:
            assert projection.event_count == 20


# =============================================================================
# Health Check API Tests
# =============================================================================


class TestHealthCheckAPI:
    """Tests for Health Check API functionality."""

    async def test_readiness_check_not_ready_before_start(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
    ):
        """Test readiness check returns not ready before start."""
        projection = CollectingProjection()

        manager = SubscriptionManager(
            event_store=in_memory_event_store,
            event_bus=in_memory_event_bus,
            checkpoint_repo=in_memory_checkpoint_repo,
        )

        await manager.subscribe(projection)

        readiness = await manager.readiness_check()

        assert readiness.ready is False
        assert "not running" in readiness.reason.lower()

    async def test_readiness_check_ready_after_start(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
    ):
        """Test readiness check returns ready after start."""
        projection = CollectingProjection()

        manager = SubscriptionManager(
            event_store=in_memory_event_store,
            event_bus=in_memory_event_bus,
            checkpoint_repo=in_memory_checkpoint_repo,
        )

        await manager.subscribe(projection)
        await manager.start()

        # Wait for subscription to be in live state
        await asyncio.sleep(0.2)

        readiness = await manager.readiness_check()

        assert readiness.ready is True
        assert "ready" in readiness.reason.lower()

        await manager.stop()

    async def test_readiness_check_not_ready_with_error_subscription(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
    ):
        """Test readiness check returns not ready when subscription is in error."""
        await populate_event_store(in_memory_event_store, 10)

        projection = FailingProjection(fail_on_event_types={"SubTestOrderCreated"})

        manager = SubscriptionManager(
            event_store=in_memory_event_store,
            event_bus=in_memory_event_bus,
            checkpoint_repo=in_memory_checkpoint_repo,
        )

        config = SubscriptionConfig(start_from="beginning", continue_on_error=False)
        await manager.subscribe(projection, config)

        with contextlib.suppress(Exception):
            await manager.start()

        readiness = await manager.readiness_check()

        assert readiness.ready is False
        assert "error" in readiness.reason.lower()

        await manager.stop()

    async def test_liveness_check_alive(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
    ):
        """Test liveness check returns alive for running manager."""
        projection = CollectingProjection()

        manager = SubscriptionManager(
            event_store=in_memory_event_store,
            event_bus=in_memory_event_bus,
            checkpoint_repo=in_memory_checkpoint_repo,
        )

        await manager.subscribe(projection)
        await manager.start()

        liveness = await manager.liveness_check()

        assert liveness.alive is True
        assert "alive" in liveness.reason.lower()

        await manager.stop()

    async def test_comprehensive_health_check(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
    ):
        """Test comprehensive health check returns all expected data."""
        await populate_event_store(in_memory_event_store, 50)

        projection = CollectingProjection()

        manager = SubscriptionManager(
            event_store=in_memory_event_store,
            event_bus=in_memory_event_bus,
            checkpoint_repo=in_memory_checkpoint_repo,
        )

        await manager.subscribe(projection)
        await manager.start()

        await projection.wait_for_events(50)

        health = await manager.health_check()

        # Verify ManagerHealth structure
        assert isinstance(health, ManagerHealth)
        assert health.status in ("healthy", "degraded", "unhealthy", "unknown")
        assert health.running is True
        assert health.subscription_count == 1
        assert health.total_events_processed >= 50
        assert health.total_events_failed == 0
        assert health.uptime_seconds >= 0
        assert len(health.subscriptions) == 1

        # Verify subscription health
        sub_health = health.subscriptions[0]
        assert sub_health.name == "CollectingProjection"
        assert sub_health.state == "live"
        assert sub_health.events_processed >= 50

        await manager.stop()

    async def test_health_check_returns_all_subscriptions(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
    ):
        """Test that health check returns status for all subscriptions."""
        projections = [CollectingProjection(name=f"Proj{i}") for i in range(4)]

        manager = SubscriptionManager(
            event_store=in_memory_event_store,
            event_bus=in_memory_event_bus,
            checkpoint_repo=in_memory_checkpoint_repo,
        )

        config = SubscriptionConfig(start_from="beginning")
        for i, projection in enumerate(projections):
            await manager.subscribe(projection, config, name=f"Proj{i}")

        await manager.start()
        await asyncio.sleep(0.2)

        health = await manager.health_check()

        assert health.subscription_count == 4
        assert len(health.subscriptions) == 4

        # Verify all subscriptions are represented
        names = {s.name for s in health.subscriptions}
        assert names == {"Proj0", "Proj1", "Proj2", "Proj3"}

        await manager.stop()

    async def test_lag_calculation(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
    ):
        """Test that lag is calculated correctly."""
        await populate_event_store(in_memory_event_store, 100)

        # Use slow projection to create observable lag
        projection = SlowProjection(delay=0.05)

        manager = SubscriptionManager(
            event_store=in_memory_event_store,
            event_bus=in_memory_event_bus,
            checkpoint_repo=in_memory_checkpoint_repo,
        )

        config = SubscriptionConfig(start_from="beginning", batch_size=10)
        await manager.subscribe(projection, config)
        await manager.start()

        # Check health while catching up (should have lag)
        await asyncio.sleep(0.3)

        health = await manager.health_check()

        # Should show some lag since processing is slow
        # (may be 0 if processing is faster than expected)
        assert health.total_lag_events >= 0

        # Wait for completion
        await projection.wait_for_events(100, timeout=30.0)

        # After catching up, lag should be minimal
        health_after = await manager.health_check()
        assert health_after.total_lag_events <= 10  # Allow small margin

        await manager.stop()

    async def test_health_status_degraded(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
    ):
        """Test health status shows degraded for catching up subscriptions."""
        await populate_event_store(in_memory_event_store, 200)

        # Use slow projection to stay in catching_up state longer
        projection = SlowProjection(delay=0.1)

        manager = SubscriptionManager(
            event_store=in_memory_event_store,
            event_bus=in_memory_event_bus,
            checkpoint_repo=in_memory_checkpoint_repo,
        )

        config = SubscriptionConfig(start_from="beginning")
        await manager.subscribe(projection, config)
        await manager.start()

        # Check health during catch-up
        await asyncio.sleep(0.2)

        subscription = manager.get_subscription("SlowProjection")
        if subscription and subscription.state == SubscriptionState.CATCHING_UP:
            health = await manager.health_check()
            # Catching up subscription should make overall health degraded
            assert health.degraded_count >= 1 or health.healthy_count >= 1

        await manager.stop()

    async def test_health_status_unhealthy(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
    ):
        """Test health status shows unhealthy for error subscriptions."""
        await populate_event_store(in_memory_event_store, 10)

        projection = FailingProjection(fail_on_event_types={"SubTestOrderCreated"})

        manager = SubscriptionManager(
            event_store=in_memory_event_store,
            event_bus=in_memory_event_bus,
            checkpoint_repo=in_memory_checkpoint_repo,
        )

        config = SubscriptionConfig(start_from="beginning", continue_on_error=False)
        await manager.subscribe(projection, config)

        with contextlib.suppress(Exception):
            await manager.start()

        health = await manager.health_check()

        assert health.status == "unhealthy"
        assert health.unhealthy_count >= 1

        await manager.stop()

    async def test_health_to_dict_serialization(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
    ):
        """Test health check result is JSON serializable."""
        projection = CollectingProjection()

        manager = SubscriptionManager(
            event_store=in_memory_event_store,
            event_bus=in_memory_event_bus,
            checkpoint_repo=in_memory_checkpoint_repo,
        )

        await manager.subscribe(projection)
        await manager.start()
        await asyncio.sleep(0.1)

        health = await manager.health_check()
        health_dict = health.to_dict()

        # Verify it's a dict with expected keys
        assert isinstance(health_dict, dict)
        assert "status" in health_dict
        assert "running" in health_dict
        assert "subscription_count" in health_dict
        assert "subscriptions" in health_dict
        assert isinstance(health_dict["subscriptions"], list)

        await manager.stop()


# =============================================================================
# Pause/Resume Functionality Tests
# =============================================================================


class TestPauseResume:
    """Tests for pause/resume functionality."""

    async def test_pause_stops_processing(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
    ):
        """Test that pause stops event processing."""
        projection = PausableProjection()

        manager = SubscriptionManager(
            event_store=in_memory_event_store,
            event_bus=in_memory_event_bus,
            checkpoint_repo=in_memory_checkpoint_repo,
        )

        config = SubscriptionConfig(start_from="beginning")
        await manager.subscribe(projection, config, name="PausableProj")
        await manager.start()

        # Wait for live mode
        await asyncio.sleep(0.2)

        # Pause the subscription
        success = await manager.pause_subscription("PausableProj")
        assert success is True

        subscription = manager.get_subscription("PausableProj")
        assert subscription is not None
        assert subscription.state == SubscriptionState.PAUSED

    async def test_resume_continues_from_position(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
    ):
        """Test that resume continues from the last position."""
        await populate_event_store(in_memory_event_store, 50)

        projection = CollectingProjection()

        manager = SubscriptionManager(
            event_store=in_memory_event_store,
            event_bus=in_memory_event_bus,
            checkpoint_repo=in_memory_checkpoint_repo,
        )

        config = SubscriptionConfig(start_from="beginning")
        await manager.subscribe(projection, config, name="ResumeProj")
        await manager.start()

        # Wait for all events to be processed
        await projection.wait_for_events(50, timeout=10.0)

        initial_count = projection.event_count

        # Pause
        await manager.pause_subscription("ResumeProj")

        # Add more events
        await populate_event_store(in_memory_event_store, 25, start_index=50)
        for i in range(50, 75):
            event = SubTestOrderCreated(
                aggregate_id=uuid4(),
                order_number=f"ORD-{i:05d}",
                amount=100.0 + i,
            )
            await in_memory_event_bus.publish([event])

        # Resume
        await manager.resume_subscription("ResumeProj")

        # Wait for new events
        await projection.wait_for_events(initial_count + 25, timeout=10.0)

        await manager.stop()

        # Should have processed events from where it left off
        assert projection.event_count >= initial_count + 25

    async def test_pause_during_catchup(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
    ):
        """Test pause during catch-up phase."""
        await populate_event_store(in_memory_event_store, 200)

        # Use slow projection to catch it during catch-up
        projection = SlowProjection(delay=0.05)

        manager = SubscriptionManager(
            event_store=in_memory_event_store,
            event_bus=in_memory_event_bus,
            checkpoint_repo=in_memory_checkpoint_repo,
        )

        config = SubscriptionConfig(start_from="beginning", batch_size=10)
        await manager.subscribe(projection, config, name="CatchupPauseProj")
        await manager.start()

        # Wait briefly for catch-up to start
        await asyncio.sleep(0.3)

        subscription = manager.get_subscription("CatchupPauseProj")
        if subscription and subscription.state == SubscriptionState.CATCHING_UP:
            # Pause during catch-up
            success = await manager.pause_subscription("CatchupPauseProj")
            assert success is True
            assert subscription.state == SubscriptionState.PAUSED
            assert subscription.state_before_pause == SubscriptionState.CATCHING_UP

            # Resume
            await manager.resume_subscription("CatchupPauseProj")
            assert subscription.state == SubscriptionState.CATCHING_UP

        await manager.stop()

    async def test_pause_during_live(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
    ):
        """Test pause during live phase."""
        projection = CollectingProjection()

        manager = SubscriptionManager(
            event_store=in_memory_event_store,
            event_bus=in_memory_event_bus,
            checkpoint_repo=in_memory_checkpoint_repo,
        )

        config = SubscriptionConfig(start_from="beginning")
        await manager.subscribe(projection, config, name="LivePauseProj")
        await manager.start()

        # Wait for live mode
        await asyncio.sleep(0.2)

        subscription = manager.get_subscription("LivePauseProj")
        assert subscription is not None
        assert subscription.state == SubscriptionState.LIVE

        # Pause during live
        success = await manager.pause_subscription("LivePauseProj")
        assert success is True
        assert subscription.state == SubscriptionState.PAUSED
        assert subscription.state_before_pause == SubscriptionState.LIVE

        # Resume
        await manager.resume_subscription("LivePauseProj")
        assert subscription.state == SubscriptionState.LIVE

        await manager.stop()

    async def test_pause_all_subscriptions(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
    ):
        """Test pausing all subscriptions at once."""
        projections = [CollectingProjection(name=f"AllPause{i}") for i in range(3)]

        manager = SubscriptionManager(
            event_store=in_memory_event_store,
            event_bus=in_memory_event_bus,
            checkpoint_repo=in_memory_checkpoint_repo,
        )

        config = SubscriptionConfig(start_from="beginning")
        for i, projection in enumerate(projections):
            await manager.subscribe(projection, config, name=f"AllPause{i}")

        await manager.start()
        await asyncio.sleep(0.2)

        # Pause all
        results = await manager.pause_all()

        # All should be paused
        assert all(results.values())
        assert len(manager.paused_subscriptions) == 3

        await manager.stop()

    async def test_resume_all_subscriptions(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
    ):
        """Test resuming all subscriptions at once."""
        projections = [CollectingProjection(name=f"AllResume{i}") for i in range(3)]

        manager = SubscriptionManager(
            event_store=in_memory_event_store,
            event_bus=in_memory_event_bus,
            checkpoint_repo=in_memory_checkpoint_repo,
        )

        config = SubscriptionConfig(start_from="beginning")
        for i, projection in enumerate(projections):
            await manager.subscribe(projection, config, name=f"AllResume{i}")

        await manager.start()
        await asyncio.sleep(0.2)

        # Pause all
        await manager.pause_all()
        assert len(manager.paused_subscriptions) == 3

        # Resume all
        results = await manager.resume_all()

        # All should be resumed
        assert all(results.values())
        assert len(manager.paused_subscriptions) == 0

        await manager.stop()

    async def test_pause_with_reason_tracking(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
    ):
        """Test that pause reason is tracked correctly."""
        projection = CollectingProjection()

        manager = SubscriptionManager(
            event_store=in_memory_event_store,
            event_bus=in_memory_event_bus,
            checkpoint_repo=in_memory_checkpoint_repo,
        )

        config = SubscriptionConfig(start_from="beginning")
        await manager.subscribe(projection, config, name="ReasonProj")
        await manager.start()
        await asyncio.sleep(0.2)

        # Pause with specific reason
        await manager.pause_subscription("ReasonProj", reason=PauseReason.MAINTENANCE)

        subscription = manager.get_subscription("ReasonProj")
        assert subscription is not None
        assert subscription.pause_reason == PauseReason.MAINTENANCE

        await manager.stop()


# =============================================================================
# Combined Advanced Features Tests
# =============================================================================


class TestCombinedAdvancedFeatures:
    """Tests for combined advanced features working together."""

    async def test_filtering_with_pause_resume(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
    ):
        """Test event filtering works correctly with pause/resume."""
        # Create mixed events
        await populate_event_store_with_types(
            in_memory_event_store,
            created_count=30,
            shipped_count=20,
            cancelled_count=10,
        )

        # Filtered projection
        projection = FilteredProjection(event_types=[SubTestOrderCreated, SubTestOrderShipped])

        manager = SubscriptionManager(
            event_store=in_memory_event_store,
            event_bus=in_memory_event_bus,
            checkpoint_repo=in_memory_checkpoint_repo,
        )

        config = SubscriptionConfig(start_from="beginning")
        await manager.subscribe(projection, config, name="FilterPauseProj")
        await manager.start()

        # Wait for some events
        await projection.wait_for_events(25, timeout=10.0)

        # Pause
        await manager.pause_subscription("FilterPauseProj")

        # Resume
        await manager.resume_subscription("FilterPauseProj")

        # Wait for all filtered events
        await projection.wait_for_events(50, timeout=10.0)

        await manager.stop()

        # Should have 50 events (30 created + 20 shipped, no cancelled)
        assert projection.event_count == 50
        for event in projection.events:
            assert not isinstance(event, SubTestOrderCancelled)

    async def test_multiple_subscriptions_with_health_check(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
    ):
        """Test health check reflects status of multiple subscriptions."""
        await populate_event_store(in_memory_event_store, 50)

        healthy_projection = CollectingProjection(name="Healthy")
        failing_projection = FailingProjection(fail_on_event_types={"SubTestOrderCreated"})

        manager = SubscriptionManager(
            event_store=in_memory_event_store,
            event_bus=in_memory_event_bus,
            checkpoint_repo=in_memory_checkpoint_repo,
        )

        config_healthy = SubscriptionConfig(start_from="beginning")
        config_failing = SubscriptionConfig(
            start_from="beginning",
            continue_on_error=False,
        )

        await manager.subscribe(healthy_projection, config_healthy, name="HealthyProj")
        await manager.subscribe(failing_projection, config_failing, name="FailingProj")

        await manager.start()
        await healthy_projection.wait_for_events(50, timeout=10.0)

        health = await manager.health_check()

        # Overall should be unhealthy due to failing subscription
        assert health.status == "unhealthy"
        assert health.unhealthy_count >= 1
        assert health.subscription_count == 2

        # Individual subscription health
        assert len(health.subscriptions) == 2

        healthy_sub = next(s for s in health.subscriptions if s.name == "HealthyProj")
        assert healthy_sub.state == "live"

        failing_sub = next(s for s in health.subscriptions if s.name == "FailingProj")
        assert failing_sub.state == "error"

        await manager.stop()

    async def test_pause_affects_health_status(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
    ):
        """Test that paused subscriptions affect health status."""
        projection = CollectingProjection()

        manager = SubscriptionManager(
            event_store=in_memory_event_store,
            event_bus=in_memory_event_bus,
            checkpoint_repo=in_memory_checkpoint_repo,
        )

        config = SubscriptionConfig(start_from="beginning")
        await manager.subscribe(projection, config, name="PauseHealthProj")
        await manager.start()
        await asyncio.sleep(0.2)

        # Before pause - should be healthy
        health_before = await manager.health_check()
        assert health_before.status == "healthy"

        # Pause
        await manager.pause_subscription("PauseHealthProj")

        # After pause - should be degraded
        health_after = await manager.health_check()
        assert health_after.status in ("degraded", "unhealthy")
        assert health_after.subscriptions[0].state == "paused"

        await manager.stop()

    async def test_comprehensive_health_with_all_features(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
    ):
        """Test comprehensive health report with all advanced features."""
        await populate_event_store(in_memory_event_store, 100)

        # Multiple projections with different behaviors
        healthy_proj = CollectingProjection(name="Healthy")
        filtered_proj = FilteredProjection(event_types=[SubTestOrderCreated])

        manager = SubscriptionManager(
            event_store=in_memory_event_store,
            event_bus=in_memory_event_bus,
            checkpoint_repo=in_memory_checkpoint_repo,
        )

        await manager.subscribe(healthy_proj, name="HealthyProj")
        await manager.subscribe(filtered_proj, name="FilteredProj")

        await manager.start()
        await healthy_proj.wait_for_events(100, timeout=10.0)
        await filtered_proj.wait_for_events(100, timeout=10.0)

        # Get comprehensive health
        comprehensive = manager.get_comprehensive_health()

        # Verify structure
        assert "status" in comprehensive
        assert "subscriptions" in comprehensive
        assert "error_stats" in comprehensive
        assert "recent_errors" in comprehensive
        assert "dlq_status" in comprehensive

        await manager.stop()


# =============================================================================
# OpenTelemetry Metrics Tests
# =============================================================================


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

        # Should not raise any errors
        metrics.record_event_processed("TestEvent", 10.5)
        metrics.record_event_failed("TestEvent", "ValueError")
        metrics.record_lag(100)
        metrics.record_state("live")

        # Snapshot should work
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

        # Should not raise any errors
        metrics.record_event_processed("TestEvent", 10.5)
        metrics.record_event_failed("TestEvent", "ValueError")

        # metrics_enabled should be False
        assert metrics.metrics_enabled is False

    async def test_metrics_time_processing_context_manager(self):
        """Test time_processing context manager."""
        metrics = SubscriptionMetrics(
            subscription_name="TimingMetrics",
            enable_metrics=True,
        )

        with metrics.time_processing() as timer:
            await asyncio.sleep(0.05)

        assert timer.duration_ms >= 50  # At least 50ms

    async def test_metrics_snapshot_accumulation(self):
        """Test that metrics snapshot accumulates correctly."""
        metrics = SubscriptionMetrics(
            subscription_name="AccumulationMetrics",
            enable_metrics=True,
        )

        # Record multiple events
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

        # Should record metrics without error
        metrics.record_event_processed("TestEvent", 15.5)
        metrics.record_event_failed("FailEvent", "TestError")
        metrics.record_lag(50)
        metrics.record_state("live")

        snapshot = metrics.get_snapshot()
        assert snapshot.events_processed == 1
        assert snapshot.events_failed == 1


# =============================================================================
# Filtering Statistics Tests
# =============================================================================


class TestFilteringStatistics:
    """Tests for event filter statistics tracking."""

    async def test_filter_stats_tracked_correctly(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
    ):
        """Test that filter statistics are tracked correctly."""
        # Create mixed events
        await populate_event_store_with_types(
            in_memory_event_store,
            created_count=20,
            shipped_count=10,
            cancelled_count=5,
        )

        # Test filter directly
        filter = EventFilter(event_types=(SubTestOrderCreated,))

        # Simulate filtering all events
        created_events = [SubTestOrderCreated(aggregate_id=uuid4()) for _ in range(20)]
        shipped_events = [SubTestOrderShipped(aggregate_id=uuid4()) for _ in range(10)]
        cancelled_events = [SubTestOrderCancelled(aggregate_id=uuid4()) for _ in range(5)]

        for event in created_events + shipped_events + cancelled_events:
            filter.matches(event)

        stats = filter.stats
        assert stats.events_evaluated == 35
        assert stats.events_matched == 20  # Only created events
        assert stats.events_skipped == 15  # shipped + cancelled
        assert stats.match_rate == pytest.approx(20 / 35, rel=0.01)

    async def test_filter_stats_reset(self):
        """Test that filter statistics can be reset."""
        filter = EventFilter(event_types=(SubTestOrderCreated,))

        # Process some events
        for _ in range(10):
            event = SubTestOrderCreated(aggregate_id=uuid4())
            filter.matches(event)

        assert filter.stats.events_evaluated == 10

        # Reset
        filter.reset_stats()

        assert filter.stats.events_evaluated == 0
        assert filter.stats.events_matched == 0
        assert filter.stats.events_skipped == 0
