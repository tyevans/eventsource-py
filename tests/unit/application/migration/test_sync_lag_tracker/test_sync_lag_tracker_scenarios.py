"""Tests for SyncLagTracker edge cases, tenant scoping, and integration scenarios."""

from datetime import UTC, datetime
from uuid import uuid4

import pytest

from eventsource.adapters.memory.store import InMemoryEventStore
from eventsource.application.migration.sync_lag_tracker import SyncLagTracker
from eventsource.ports import ExpectedVersion, Position
from eventsource.ports.migration.models import MigrationConfig, SyncLag
from tests.unit.application.migration.test_sync_lag_tracker._fixtures import (
    LagTestEvent,
    seed_events,
    sid,
)


class TestEdgeCases:
    """Tests for edge cases and boundary conditions."""

    @pytest.mark.asyncio
    async def test_empty_stores(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        config: MigrationConfig,
    ) -> None:
        """Test with an empty source feed."""
        tracker = SyncLagTracker(
            source_store=source_store,
            target_store=target_store,
            config=config,
            enable_tracing=False,
        )

        lag = await tracker.calculate_lag()

        assert lag.events == 0
        assert tracker.is_sync_ready() is True
        assert tracker.is_fully_converged() is True

    @pytest.mark.asyncio
    async def test_large_lag_is_bounded(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        strict_config: MigrationConfig,
    ) -> None:
        """A very large backlog is capped at threshold + 1 and marked bounded."""
        tracker = SyncLagTracker(
            source_store=source_store,
            target_store=target_store,
            config=strict_config,  # threshold = 10
            enable_tracing=False,
        )
        await seed_events(source_store, 1_000)

        lag = await tracker.calculate_lag()

        assert lag.events == strict_config.cutover_max_lag_events + 1
        assert lag.count_is_bounded is True
        assert tracker.is_sync_ready() is False
        # A looser override cannot be honored against a bounded count: 11
        # here stands for 1_000, so `max_lag=1_000` would answer True on
        # no evidence.
        with pytest.raises(ValueError, match="exceeds the configured"):
            tracker.is_converged(max_lag=1_000)

    @pytest.mark.asyncio
    async def test_exact_threshold(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        config: MigrationConfig,
    ) -> None:
        """Test when lag is exactly at threshold."""
        # Config threshold is 100, source has exactly 100 events behind.
        tracker = SyncLagTracker(
            source_store=source_store,
            target_store=target_store,
            config=config,  # threshold = 100
            enable_tracing=False,
        )
        await seed_events(source_store, 100)

        await tracker.calculate_lag()  # lag = 100

        assert tracker.is_converged() is True  # <= threshold
        assert tracker.is_sync_ready() is True

    def test_convergence_with_few_samples(
        self,
        tracker: SyncLagTracker,
    ) -> None:
        """Test convergence detection with very few samples."""
        # Need at least 4 samples for convergence detection
        for i in range(3):
            lag = SyncLag(
                events=50 - (i * 10),
                source_position=Position(store_id="source", key=(100,)),
                target_position=Position(store_id="target", key=(50 + (i * 10),)),
                timestamp=datetime.now(UTC),
            )
            tracker.record_lag(lag)

        stats = tracker.get_lag_stats()

        # With fewer than 4 samples, should not detect convergence
        assert stats.is_converging is False


class TestWithTenantId:
    """Tests for tracker with tenant ID."""

    @pytest.mark.asyncio
    async def test_tracker_with_tenant_id(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        config: MigrationConfig,
    ) -> None:
        """Test tracker operations with tenant ID set."""
        tenant_id = uuid4()
        tracker = SyncLagTracker(
            source_store=source_store,
            target_store=target_store,
            config=config,
            tenant_id=tenant_id,
            enable_tracing=False,
        )
        # Only events for this tenant should count toward the lag.
        for _ in range(50):
            await source_store.append(
                sid(),
                [(lambda s: LagTestEvent(aggregate_id=s.aggregate_id, tenant_id=tenant_id))(sid())],
                ExpectedVersion.any_(),
            )
        for _ in range(5):
            await source_store.append(
                sid(),
                [(lambda s: LagTestEvent(aggregate_id=s.aggregate_id, tenant_id=uuid4()))(sid())],
                ExpectedVersion.any_(),
            )

        lag = await tracker.calculate_lag()

        assert tracker.tenant_id == tenant_id
        assert lag.events == 50


class TestIntegrationScenarios:
    """Tests for realistic integration scenarios."""

    @pytest.mark.asyncio
    async def test_typical_migration_scenario(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        config: MigrationConfig,
    ) -> None:
        """Test a typical migration scenario with decreasing lag as the
        target catches up on a fixed source feed of 100 events."""
        tracker = SyncLagTracker(
            source_store=source_store,
            target_store=target_store,
            config=config,
            enable_tracing=False,
        )

        positions = []
        for _ in range(100):
            result = await source_store.append(
                (stream := sid()),
                [LagTestEvent(aggregate_id=stream.aggregate_id)],
                ExpectedVersion.any_(),
            )
            positions.append(result.position)

        # Simulate sync progress: target copies more of the source feed each pass.
        copied_counts = [0, 20, 40, 60, 80, 90, 95, 99, 100]

        for copied in copied_counts:
            since = positions[copied - 1] if copied > 0 else None
            await tracker.calculate_lag(since=since)

        # Final state
        assert tracker.is_sync_ready() is True
        assert tracker.is_fully_converged() is True

        stats = tracker.get_lag_stats()
        assert stats.is_converging is True
        assert stats.current_lag == 0
        assert stats.max_lag == 100
        assert stats.min_lag == 0

    @pytest.mark.asyncio
    async def test_fluctuating_lag_scenario(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        config: MigrationConfig,
    ) -> None:
        """Test scenario where lag fluctuates but generally improves."""
        tracker = SyncLagTracker(
            source_store=source_store,
            target_store=target_store,
            config=config,
            enable_tracing=False,
        )

        positions = []
        for _ in range(100):
            result = await source_store.append(
                (stream := sid()),
                [LagTestEvent(aggregate_id=stream.aggregate_id)],
                ExpectedVersion.any_(),
            )
            positions.append(result.position)

        # Fluctuating but improving progress through the fixed source feed.
        copied_counts = [0, 30, 25, 50, 45, 70, 65, 90, 95, 100]

        for copied in copied_counts:
            since = positions[copied - 1] if copied > 0 else None
            await tracker.calculate_lag(since=since)

        stats = tracker.get_lag_stats()

        # Despite fluctuations, overall trend is converging
        assert stats.is_converging is True
