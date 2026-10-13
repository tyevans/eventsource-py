"""Tests for test harness reset functionality."""

from __future__ import annotations

from uuid import uuid4

import pytest

from eventsource.domain import StreamId
from eventsource.ports import ExpectedVersion
from eventsource.ports.positions import Position
from eventsource.testing import InMemoryTestHarness
from tests.unit.testing.test_harness._fixtures import HarnessSampleEvent


class TestHarnessReset:
    """Tests for reset functionality."""

    @pytest.fixture
    def harness(self) -> InMemoryTestHarness:
        """Create a fresh harness for each test."""
        return InMemoryTestHarness()

    @pytest.mark.asyncio
    async def test_reset_creates_new_event_store(self, harness: InMemoryTestHarness) -> None:
        """reset() creates a new event store instance."""
        old_store = harness.event_store
        aggregate_id = uuid4()
        event = HarnessSampleEvent(
            aggregate_id=aggregate_id,
            aggregate_type="Sample",
            aggregate_version=1,
        )
        await old_store.append(
            StreamId(aggregate_id=aggregate_id, category="Sample"),
            [event],
            ExpectedVersion.no_stream(),
        )

        harness.reset()

        assert harness.event_store is not old_store
        # New store should be empty
        envelopes = [e async for e in harness.event_store.read_all()]
        assert len(envelopes) == 0

    @pytest.mark.asyncio
    async def test_reset_creates_new_event_bus(self, harness: InMemoryTestHarness) -> None:
        """reset() creates a new event bus instance."""
        old_bus = harness.event_bus
        event = HarnessSampleEvent(
            aggregate_id=uuid4(),
            aggregate_type="Sample",
            aggregate_version=1,
        )
        await old_bus.publish([event])

        harness.reset()

        assert harness.event_bus is not old_bus
        assert harness.published_events == []

    def test_reset_creates_new_checkpoint_repo(self, harness: InMemoryTestHarness) -> None:
        """reset() creates a new checkpoint repository."""
        old_repo = harness.checkpoint_repo
        harness.reset()
        assert harness.checkpoint_repo is not old_repo

    def test_reset_creates_new_dlq_repo(self, harness: InMemoryTestHarness) -> None:
        """reset() creates a new DLQ repository."""
        old_repo = harness.dlq_repo
        harness.reset()
        assert harness.dlq_repo is not old_repo

    @pytest.mark.asyncio
    async def test_reset_clears_checkpoint_data(self, harness: InMemoryTestHarness) -> None:
        """reset() clears checkpoint repository data."""
        event_id = uuid4()
        await harness.checkpoint_repo.save_position(
            subscription_id="test-projection",
            position=Position(store_id=harness.event_store.store_id, key=(42,)),
            event_id=event_id,
            event_type="TestEvent",
        )

        harness.reset()

        position = await harness.checkpoint_repo.get_position("test-projection")
        assert position is None

    @pytest.mark.asyncio
    async def test_reset_clears_dlq_data(self, harness: InMemoryTestHarness) -> None:
        """reset() clears DLQ repository data."""
        await harness.dlq_repo.add_failed_event(
            event_id=uuid4(),
            projection_name="test-projection",
            event_type="TestEvent",
            event_data={},
            error=Exception("Test error"),
        )

        harness.reset()

        entries = await harness.dlq_repo.get_failed_events()
        assert len(entries) == 0
