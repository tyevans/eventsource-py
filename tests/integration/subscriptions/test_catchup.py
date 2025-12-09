"""
Integration tests for catch-up functionality.

Tests cover:
- Catch-up from beginning processes all events
- Catch-up from checkpoint resumes correctly
- Catch-up with different batch sizes
- Empty event store handling
- Event ordering during catch-up
"""

import pytest

from eventsource.subscriptions import SubscriptionConfig

from .conftest import (
    CollectingProjection,
    SubTestOrderCreated,
    populate_event_store,
    populate_event_store_with_types,
)

pytestmark = pytest.mark.asyncio


class TestCatchUpFromBeginning:
    """Tests for catch-up from the beginning of the event store."""

    async def test_catchup_from_beginning_processes_all_events(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        subscription_manager,
    ):
        """Test catch-up from beginning processes all historical events."""
        # Arrange: Populate store with 100 events
        await populate_event_store(in_memory_event_store, 100)

        # Create projection
        projection = CollectingProjection()

        # Act: Subscribe and start
        config = SubscriptionConfig(start_from="beginning")
        await subscription_manager.subscribe(projection, config)
        await subscription_manager.start()

        # Assert: All events processed
        success = await projection.wait_for_events(100)
        assert success, f"Expected 100 events, got {projection.event_count}"
        assert projection.event_count == 100

        # Verify order - events should be in order
        for i, event in enumerate(projection.events):
            assert event.order_number == f"ORD-{i:05d}"

    async def test_catchup_preserves_event_order(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        subscription_manager,
    ):
        """Test catch-up maintains correct event ordering."""
        # Arrange: Populate with mixed events
        events = await populate_event_store_with_types(
            in_memory_event_store,
            created_count=30,
            shipped_count=20,
            cancelled_count=10,
        )

        projection = CollectingProjection()

        # Act
        config = SubscriptionConfig(start_from="beginning")
        await subscription_manager.subscribe(projection, config)
        await subscription_manager.start()

        # Assert
        success = await projection.wait_for_events(60)
        assert success
        assert projection.event_count == 60

        # Verify events are in append order
        for i, (original, received) in enumerate(zip(events, projection.events, strict=True)):
            assert original.event_id == received.event_id, f"Mismatch at position {i}"

    async def test_catchup_with_large_event_count(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        subscription_manager,
    ):
        """Test catch-up handles large number of events correctly."""
        # Arrange: Populate store with 500 events
        await populate_event_store(in_memory_event_store, 500)

        projection = CollectingProjection()

        # Act: Subscribe with default batch size
        config = SubscriptionConfig(start_from="beginning")
        await subscription_manager.subscribe(projection, config)
        await subscription_manager.start()

        # Assert: All events processed
        success = await projection.wait_for_events(500, timeout=15.0)
        assert success, f"Expected 500 events, got {projection.event_count}"
        assert projection.event_count == 500


class TestCatchUpFromCheckpoint:
    """Tests for resuming from a checkpoint."""

    async def test_catchup_from_checkpoint(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        subscription_manager,
    ):
        """Test catch-up from checkpoint resumes at correct position."""
        # Arrange: Populate store with 100 events
        events = await populate_event_store(in_memory_event_store, 100)

        # Set checkpoint at position 50 (meaning we've processed up to position 50)
        await in_memory_checkpoint_repo.save_position(
            subscription_id="CollectingProjection",
            position=50,
            event_id=events[49].event_id,
            event_type="TestOrderCreated",
        )

        projection = CollectingProjection()

        # Act: Subscribe with checkpoint start
        config = SubscriptionConfig(start_from="checkpoint")
        await subscription_manager.subscribe(projection, config)
        await subscription_manager.start()

        # Assert: Only events after checkpoint processed
        success = await projection.wait_for_events(50)
        assert success, f"Expected 50 events, got {projection.event_count}"
        assert projection.event_count == 50

        # First event should be at position 51 (0-indexed: 50)
        assert projection.events[0].order_number == "ORD-00050"

    async def test_catchup_from_checkpoint_no_checkpoint_exists(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        subscription_manager,
    ):
        """Test checkpoint mode with no existing checkpoint starts from beginning."""
        # Arrange: Populate store with events but no checkpoint
        await populate_event_store(in_memory_event_store, 50)

        projection = CollectingProjection()

        # Act: Subscribe with checkpoint start but no checkpoint exists
        config = SubscriptionConfig(start_from="checkpoint")
        await subscription_manager.subscribe(projection, config)
        await subscription_manager.start()

        # Assert: All events processed (defaults to beginning)
        success = await projection.wait_for_events(50)
        assert success
        assert projection.event_count == 50

    async def test_catchup_from_specific_position(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        subscription_manager,
    ):
        """Test catch-up from a specific position."""
        # Arrange: Populate store with 100 events
        await populate_event_store(in_memory_event_store, 100)

        projection = CollectingProjection()

        # Act: Subscribe starting from position 75
        config = SubscriptionConfig(start_from=75)
        await subscription_manager.subscribe(projection, config)
        await subscription_manager.start()

        # Assert: Only events from position 76 onwards processed
        success = await projection.wait_for_events(25)
        assert success
        assert projection.event_count == 25
        assert projection.events[0].order_number == "ORD-00075"


class TestCatchUpWithBatchSize:
    """Tests for catch-up with different batch sizes."""

    async def test_catchup_with_small_batch_size(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        subscription_manager,
    ):
        """Test catch-up with small batch size processes correctly."""
        # Arrange: Populate store with 100 events
        await populate_event_store(in_memory_event_store, 100)

        projection = CollectingProjection()

        # Act: Subscribe with small batch size
        config = SubscriptionConfig(start_from="beginning", batch_size=10)
        await subscription_manager.subscribe(projection, config)
        await subscription_manager.start()

        # Assert: All events processed
        success = await projection.wait_for_events(100)
        assert success
        assert projection.event_count == 100

    async def test_catchup_with_large_batch_size(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        subscription_manager,
    ):
        """Test catch-up with large batch size processes correctly."""
        # Arrange: Populate store with 250 events
        await populate_event_store(in_memory_event_store, 250)

        projection = CollectingProjection()

        # Act: Subscribe with large batch size
        config = SubscriptionConfig(start_from="beginning", batch_size=500)
        await subscription_manager.subscribe(projection, config)
        await subscription_manager.start()

        # Assert: All events processed
        success = await projection.wait_for_events(250, timeout=10.0)
        assert success
        assert projection.event_count == 250

    async def test_catchup_batch_size_one(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        subscription_manager,
    ):
        """Test catch-up with batch size of 1."""
        # Arrange: Populate store with 25 events
        await populate_event_store(in_memory_event_store, 25)

        projection = CollectingProjection()

        # Act: Subscribe with batch size 1
        config = SubscriptionConfig(start_from="beginning", batch_size=1)
        await subscription_manager.subscribe(projection, config)
        await subscription_manager.start()

        # Assert: All events processed
        success = await projection.wait_for_events(25, timeout=10.0)
        assert success
        assert projection.event_count == 25


class TestEmptyEventStore:
    """Tests for catch-up with empty event store."""

    async def test_empty_event_store_goes_to_live(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        subscription_manager,
    ):
        """Test catch-up with empty event store transitions to live."""
        projection = CollectingProjection()

        # Act: Subscribe and start with empty store
        config = SubscriptionConfig(start_from="beginning")
        await subscription_manager.subscribe(projection, config)
        await subscription_manager.start()

        # Assert: No events, but subscription is running
        assert projection.event_count == 0

        subscription = subscription_manager.get_subscription("CollectingProjection")
        assert subscription is not None
        # Should be in live state (caught up with empty store)
        assert subscription.state.value == "live"

    async def test_empty_store_receives_live_events(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        subscription_manager,
    ):
        """Test subscription on empty store can receive live events."""
        projection = CollectingProjection()

        # Act: Subscribe and start with empty store
        config = SubscriptionConfig(start_from="beginning")
        await subscription_manager.subscribe(projection, config)
        await subscription_manager.start()

        # Publish live event
        from uuid import uuid4

        live_event = SubTestOrderCreated(
            aggregate_id=uuid4(),
            order_number="LIVE-00001",
            amount=500.0,
        )
        await in_memory_event_store.append_events(
            aggregate_id=live_event.aggregate_id,
            aggregate_type="SubTestOrder",
            events=[live_event],
            expected_version=0,
        )
        await in_memory_event_bus.publish([live_event])

        # Assert: Live event received
        import asyncio

        await asyncio.sleep(0.1)
        success = await projection.wait_for_events(1)
        assert success
        assert projection.events[0].order_number == "LIVE-00001"


class TestNoDuplicateEvents:
    """Tests ensuring no duplicate events during catch-up."""

    async def test_no_duplicates_during_catchup(
        self,
        in_memory_event_store,
        in_memory_event_bus,
        in_memory_checkpoint_repo,
        subscription_manager,
    ):
        """Test no events are duplicated during catch-up."""
        # Arrange: Populate store with events
        await populate_event_store(in_memory_event_store, 100)

        projection = CollectingProjection()

        # Act: Subscribe and start
        config = SubscriptionConfig(start_from="beginning")
        await subscription_manager.subscribe(projection, config)
        await subscription_manager.start()

        # Wait for completion
        success = await projection.wait_for_events(100)
        assert success

        # Assert: Exactly 100 events, no duplicates
        assert projection.event_count == 100
        assert not projection.has_duplicates()

        # Verify all event IDs are unique
        event_ids = [str(e.event_id) for e in projection.events]
        assert len(event_ids) == len(set(event_ids))
