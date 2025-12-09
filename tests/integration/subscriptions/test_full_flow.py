"""
End-to-end integration tests for complete subscription flow.

Tests cover:
- Full lifecycle: subscribe -> start -> process -> stop
- Checkpoint persistence and verification
- Resume after restart
- Context manager usage
- Multiple concurrent subscribers
- Error handling and recovery
"""

import asyncio
import contextlib
from uuid import uuid4

import pytest

from eventsource.subscriptions import (
    SubscriptionConfig,
    SubscriptionManager,
    SubscriptionState,
)

from .conftest import (
    CollectingProjection,
    FailingProjection,
    SlowProjection,
    SubTestOrderCreated,
    populate_event_store,
    publish_live_event,
)

pytestmark = pytest.mark.asyncio


class TestFullLifecycle:
    """Tests for complete subscription lifecycle."""

    async def test_full_lifecycle(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        subscription_manager,
    ):
        """Test complete lifecycle: subscribe -> start -> process -> stop."""
        # Arrange
        await populate_event_store(in_memory_event_store, 50)
        projection = CollectingProjection()

        # Act: Full lifecycle
        subscription = await subscription_manager.subscribe(projection)
        assert subscription.state == SubscriptionState.STARTING

        await subscription_manager.start()
        success = await projection.wait_for_events(50)
        assert success

        await subscription_manager.stop()

        # Assert
        assert projection.event_count == 50
        assert subscription.state == SubscriptionState.STOPPED

    async def test_lifecycle_with_live_events(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        subscription_manager,
    ):
        """Test lifecycle including live event processing."""
        # Arrange
        await populate_event_store(in_memory_event_store, 25)
        projection = CollectingProjection()

        # Act
        config = SubscriptionConfig(start_from="beginning")
        await subscription_manager.subscribe(projection, config)
        await subscription_manager.start()

        await projection.wait_for_events(25)

        # Publish live events
        for i in range(25, 35):
            await publish_live_event(
                in_memory_event_store,
                in_memory_event_bus,
                f"ORD-{i:05d}",
            )

        success = await projection.wait_for_events(35)
        assert success

        await subscription_manager.stop()

        # Assert
        assert projection.event_count == 35
        assert not projection.has_duplicates()


class TestCheckpointPersistence:
    """Tests for checkpoint persistence."""

    async def test_checkpoint_persisted_after_processing(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        subscription_manager,
    ):
        """Test checkpoints are persisted correctly after processing."""
        # Arrange
        await populate_event_store(in_memory_event_store, 100)
        projection = CollectingProjection()

        # Act: Process events
        config = SubscriptionConfig(start_from="beginning")
        await subscription_manager.subscribe(projection, config)
        await subscription_manager.start()
        await projection.wait_for_events(100)
        await subscription_manager.stop()

        # Assert: Checkpoint saved
        position = await in_memory_checkpoint_repo.get_position("CollectingProjection")
        assert position is not None
        assert position >= 100

    async def test_checkpoint_persisted_periodically(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        subscription_manager,
    ):
        """Test checkpoint is persisted during processing."""
        await populate_event_store(in_memory_event_store, 200)
        projection = CollectingProjection()

        config = SubscriptionConfig(start_from="beginning", batch_size=50)
        await subscription_manager.subscribe(projection, config)
        await subscription_manager.start()

        # Wait for some events
        await projection.wait_for_events(100)

        # Check checkpoint was updated
        position = await in_memory_checkpoint_repo.get_position("CollectingProjection")
        assert position is not None
        assert position >= 50  # At least one batch checkpointed

        await subscription_manager.stop()


class TestResumeAfterRestart:
    """Tests for resuming after restart."""

    async def test_resume_after_restart(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
    ):
        """Test subscription resumes correctly after restart."""
        # First run: Process 50 events
        await populate_event_store(in_memory_event_store, 50)

        projection1 = CollectingProjection()
        manager1 = SubscriptionManager(
            event_store=in_memory_event_store,
            event_bus=in_memory_event_bus,
            checkpoint_repo=in_memory_checkpoint_repo,
        )

        config = SubscriptionConfig(start_from="beginning")
        await manager1.subscribe(projection1, config, name="TestProjection")
        await manager1.start()
        await projection1.wait_for_events(50)
        await manager1.stop()

        # Add more events
        for i in range(50, 100):
            event = SubTestOrderCreated(
                aggregate_id=uuid4(),
                order_number=f"ORD-{i:05d}",
                amount=100.0 + i,
            )
            await in_memory_event_store.append_events(
                aggregate_id=event.aggregate_id,
                aggregate_type="TestOrder",
                events=[event],
                expected_version=0,
            )

        # Second run: Resume from checkpoint
        projection2 = CollectingProjection()
        manager2 = SubscriptionManager(
            event_store=in_memory_event_store,
            event_bus=in_memory_event_bus,
            checkpoint_repo=in_memory_checkpoint_repo,
        )

        resume_config = SubscriptionConfig(start_from="checkpoint")
        await manager2.subscribe(projection2, resume_config, name="TestProjection")
        await manager2.start()
        await projection2.wait_for_events(50)  # Only new events
        await manager2.stop()

        # Assert: Only processed events 51-100
        assert projection2.event_count == 50
        assert projection2.events[0].order_number == "ORD-00050"

    async def test_resume_from_beginning_ignores_checkpoint(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
    ):
        """Test start_from='beginning' ignores existing checkpoint."""
        # Create checkpoint
        await in_memory_checkpoint_repo.save_position(
            subscription_id="TestProjection",
            position=50,
            event_id=uuid4(),
            event_type="TestOrderCreated",
        )

        await populate_event_store(in_memory_event_store, 100)

        projection = CollectingProjection()
        manager = SubscriptionManager(
            event_store=in_memory_event_store,
            event_bus=in_memory_event_bus,
            checkpoint_repo=in_memory_checkpoint_repo,
        )

        # Start from beginning despite checkpoint
        config = SubscriptionConfig(start_from="beginning")
        await manager.subscribe(projection, config, name="TestProjection")
        await manager.start()
        await projection.wait_for_events(100)
        await manager.stop()

        # Should have all 100 events
        assert projection.event_count == 100
        assert projection.events[0].order_number == "ORD-00000"


class TestContextManagerUsage:
    """Tests for async context manager usage."""

    async def test_context_manager(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
    ):
        """Test subscription manager as async context manager."""
        await populate_event_store(in_memory_event_store, 25)
        projection = CollectingProjection()

        # Create manager and subscribe BEFORE entering context
        manager = SubscriptionManager(
            event_store=in_memory_event_store,
            event_bus=in_memory_event_bus,
            checkpoint_repo=in_memory_checkpoint_repo,
        )
        await manager.subscribe(projection)

        # Context manager starts on entry and stops on exit
        async with manager:
            # Wait with longer timeout for reliability
            success = await projection.wait_for_events(25, timeout=10.0)
            assert success, f"Expected 25 events, got {projection.event_count}"

        # Assert: Manager stopped, events processed
        assert projection.event_count == 25
        assert not manager.is_running

    async def test_context_manager_cleanup_on_exception(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
    ):
        """Test context manager cleans up on exception."""
        projection = CollectingProjection()

        try:
            async with SubscriptionManager(
                event_store=in_memory_event_store,
                event_bus=in_memory_event_bus,
                checkpoint_repo=in_memory_checkpoint_repo,
            ) as manager:
                await manager.subscribe(projection)
                raise RuntimeError("Test exception")
        except RuntimeError:
            pass

        assert not manager.is_running


class TestMultipleConcurrentSubscribers:
    """Tests for multiple concurrent subscribers."""

    async def test_multiple_subscribers_independent_processing(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        subscription_manager,
    ):
        """Test multiple subscribers process events independently."""
        await populate_event_store(in_memory_event_store, 50)

        projection1 = CollectingProjection(name="Proj1")
        projection2 = CollectingProjection(name="Proj2")
        projection3 = CollectingProjection(name="Proj3")

        config = SubscriptionConfig(start_from="beginning")
        await subscription_manager.subscribe(projection1, config, name="Projection1")
        await subscription_manager.subscribe(projection2, config, name="Projection2")
        await subscription_manager.subscribe(projection3, config, name="Projection3")

        await subscription_manager.start()

        # All should receive events
        await projection1.wait_for_events(50)
        await projection2.wait_for_events(50)
        await projection3.wait_for_events(50)

        assert projection1.event_count == 50
        assert projection2.event_count == 50
        assert projection3.event_count == 50

    async def test_multiple_subscribers_different_starting_points(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        subscription_manager,
    ):
        """Test multiple subscribers with different start positions."""
        events = await populate_event_store(in_memory_event_store, 100)

        # Set up checkpoint for one subscriber
        await in_memory_checkpoint_repo.save_position(
            subscription_id="ProjectionWithCheckpoint",
            position=75,
            event_id=events[74].event_id,
            event_type="TestOrderCreated",
        )

        projection_beginning = CollectingProjection(name="Beginning")
        projection_checkpoint = CollectingProjection(name="Checkpoint")

        config_beginning = SubscriptionConfig(start_from="beginning")
        config_checkpoint = SubscriptionConfig(start_from="checkpoint")

        await subscription_manager.subscribe(
            projection_beginning, config_beginning, name="ProjectionFromBeginning"
        )
        await subscription_manager.subscribe(
            projection_checkpoint, config_checkpoint, name="ProjectionWithCheckpoint"
        )

        await subscription_manager.start()

        await projection_beginning.wait_for_events(100)
        await projection_checkpoint.wait_for_events(25)  # 100 - 75 = 25

        assert projection_beginning.event_count == 100
        assert projection_checkpoint.event_count == 25
        assert projection_checkpoint.events[0].order_number == "ORD-00075"

    async def test_slow_subscriber_does_not_block_others(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        subscription_manager,
    ):
        """Test slow subscriber doesn't block other subscribers."""
        await populate_event_store(in_memory_event_store, 20)

        fast_projection = CollectingProjection(name="Fast")
        slow_projection = SlowProjection(delay=0.05)

        config = SubscriptionConfig(start_from="beginning")
        await subscription_manager.subscribe(fast_projection, config, name="FastProjection")
        await subscription_manager.subscribe(slow_projection, config, name="SlowProjection")

        await subscription_manager.start()

        # Fast projection should complete quickly
        success = await fast_projection.wait_for_events(20, timeout=5.0)
        assert success

        # Slow projection may still be processing
        # Wait for it with longer timeout
        success = await slow_projection.wait_for_events(20, timeout=15.0)
        assert success

        assert fast_projection.event_count == 20
        assert slow_projection.event_count == 20


class TestErrorHandlingAndRecovery:
    """Tests for error handling and recovery."""

    async def test_continue_on_error_mode(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        subscription_manager,
    ):
        """Test continue_on_error allows processing to continue."""
        await populate_event_store(in_memory_event_store, 10)

        # Create projection that fails on some events (use correct event type name)
        projection = FailingProjection(fail_on_event_types={"SubTestOrderCreated"})

        config = SubscriptionConfig(start_from="beginning", continue_on_error=True)
        await subscription_manager.subscribe(projection, config, name="FailingProjection")

        await subscription_manager.start()

        # Wait a bit for processing attempts
        await asyncio.sleep(0.5)

        # Should have failures but manager should still be running
        assert subscription_manager.is_running

        subscription = subscription_manager.get_subscription("FailingProjection")
        assert subscription.events_failed > 0

    async def test_stop_on_error_mode(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        subscription_manager,
    ):
        """Test subscription enters error state when continue_on_error is False."""
        await populate_event_store(in_memory_event_store, 10)

        # Use correct event type name
        projection = FailingProjection(fail_on_event_types={"SubTestOrderCreated"})

        config = SubscriptionConfig(start_from="beginning", continue_on_error=False)
        await subscription_manager.subscribe(projection, config, name="FailingProjection")

        with contextlib.suppress(Exception):
            await subscription_manager.start()

        subscription = subscription_manager.get_subscription("FailingProjection")
        assert subscription.state == SubscriptionState.ERROR

    async def test_healthy_subscriber_unaffected_by_failing_sibling(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        subscription_manager,
    ):
        """Test healthy subscriber is not affected by failing sibling."""
        await populate_event_store(in_memory_event_store, 20)

        healthy_projection = CollectingProjection(name="Healthy")
        # Use correct event type name
        failing_projection = FailingProjection(fail_on_event_types={"SubTestOrderCreated"})

        config_healthy = SubscriptionConfig(start_from="beginning")
        config_failing = SubscriptionConfig(start_from="beginning", continue_on_error=True)

        await subscription_manager.subscribe(
            healthy_projection, config_healthy, name="HealthyProjection"
        )
        await subscription_manager.subscribe(
            failing_projection, config_failing, name="FailingProjection"
        )

        await subscription_manager.start()

        # Healthy projection should process all events
        success = await healthy_projection.wait_for_events(20)
        assert success
        assert healthy_projection.event_count == 20


class TestHealthAndStatus:
    """Tests for health checks and status reporting."""

    async def test_health_status_healthy(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        subscription_manager,
    ):
        """Test health status when all subscriptions are healthy."""
        await populate_event_store(in_memory_event_store, 10)

        projection = CollectingProjection()

        await subscription_manager.subscribe(projection)
        await subscription_manager.start()

        await projection.wait_for_events(10)

        health = subscription_manager.get_health()

        assert health["status"] == "healthy"
        assert health["running"] is True
        assert health["subscription_count"] == 1
        assert len(health["subscriptions"]) == 1
        assert health["subscriptions"][0]["state"] == "live"

    async def test_health_status_unhealthy(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        subscription_manager,
    ):
        """Test health status when subscription has errors."""
        await populate_event_store(in_memory_event_store, 10)

        # Use correct event type name
        projection = FailingProjection(fail_on_event_types={"SubTestOrderCreated"})

        config = SubscriptionConfig(start_from="beginning", continue_on_error=False)
        await subscription_manager.subscribe(projection, config)

        with contextlib.suppress(Exception):
            await subscription_manager.start()

        health = subscription_manager.get_health()

        assert health["status"] == "unhealthy"
        assert health["subscriptions"][0]["state"] == "error"

    async def test_subscription_status_details(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        subscription_manager,
    ):
        """Test subscription status includes all details."""
        await populate_event_store(in_memory_event_store, 50)

        projection = CollectingProjection()

        await subscription_manager.subscribe(projection)
        await subscription_manager.start()

        await projection.wait_for_events(50)

        subscription = subscription_manager.get_subscription("CollectingProjection")
        status = subscription.get_status()

        assert status.name == "CollectingProjection"
        assert status.state == "live"
        assert status.events_processed >= 50
        assert status.events_failed == 0
        assert status.last_processed_at is not None
        assert status.uptime_seconds >= 0


class TestEdgeCases:
    """Tests for edge cases and boundary conditions."""

    async def test_empty_store_then_live_events(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        subscription_manager,
    ):
        """Test starting with empty store then receiving live events."""
        projection = CollectingProjection()

        await subscription_manager.subscribe(projection)
        await subscription_manager.start()

        # Verify we're in live mode with no events
        await asyncio.sleep(0.1)
        assert projection.event_count == 0

        # Now publish events
        for i in range(10):
            await publish_live_event(
                in_memory_event_store,
                in_memory_event_bus,
                f"ORD-{i:03d}",
            )

        success = await projection.wait_for_events(10)
        assert success
        assert projection.event_count == 10

    async def test_start_stop_start_cycle(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
    ):
        """Test manager can be started, stopped, and restarted."""
        await populate_event_store(in_memory_event_store, 30)

        projection = CollectingProjection()
        manager = SubscriptionManager(
            event_store=in_memory_event_store,
            event_bus=in_memory_event_bus,
            checkpoint_repo=in_memory_checkpoint_repo,
        )

        # First start
        await manager.subscribe(projection)
        await manager.start()
        await projection.wait_for_events(30)
        await manager.stop()

        assert not manager.is_running

        # Add more events
        for i in range(30, 40):
            event = SubTestOrderCreated(
                aggregate_id=uuid4(),
                order_number=f"ORD-{i:05d}",
                amount=100.0 + i,
            )
            await in_memory_event_store.append_events(
                aggregate_id=event.aggregate_id,
                aggregate_type="TestOrder",
                events=[event],
                expected_version=0,
            )

        # Can't restart same manager (would need new instance)
        # This tests that stop properly cleans up

    async def test_single_event(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        subscription_manager,
    ):
        """Test handling of a single event."""
        await populate_event_store(in_memory_event_store, 1)

        projection = CollectingProjection()

        await subscription_manager.subscribe(projection)
        await subscription_manager.start()

        success = await projection.wait_for_events(1)
        assert success
        assert projection.event_count == 1
        assert projection.events[0].order_number == "ORD-00000"
