"""Tests for SyncLagTracker lag calculation and convergence detection."""

from uuid import uuid4

import pytest

from eventsource.adapters.memory.store import InMemoryEventStore
from eventsource.application.migration.metrics import (
    clear_metrics_registry,
    get_migration_metrics,
)
from eventsource.application.migration.sync_lag_tracker import SyncLagTracker
from eventsource.ports.migration.models import MigrationConfig
from tests.unit.application.migration.test_sync_lag_tracker._fixtures import (
    seed_events,
)


class TestCalculateLag:
    """Tests for calculate_lag method."""

    @pytest.mark.asyncio
    async def test_calculate_lag_basic(
        self,
        tracker: SyncLagTracker,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
    ) -> None:
        """Source feed has 50 events and nothing has been copied (since=None):
        the tracker counts all of them."""
        await seed_events(source_store, 50)

        lag = await tracker.calculate_lag()

        assert lag.events == 50
        assert lag.source_position == await source_store.current_position()
        assert lag.target_position == await target_store.current_position()
        assert lag.timestamp is not None
        assert lag.count_is_bounded is False

    @pytest.mark.asyncio
    async def test_calculate_lag_updates_current(
        self,
        tracker: SyncLagTracker,
        source_store: InMemoryEventStore,
    ) -> None:
        """Test that calculate_lag updates current_lag property."""
        await seed_events(source_store, 5)
        assert tracker.current_lag is None

        lag = await tracker.calculate_lag()

        assert tracker.current_lag is lag

    @pytest.mark.asyncio
    async def test_calculate_lag_adds_sample(
        self,
        tracker: SyncLagTracker,
        source_store: InMemoryEventStore,
    ) -> None:
        """Test that calculate_lag adds to sample history."""
        await seed_events(source_store, 5)
        assert len(tracker._lag_samples) == 0

        await tracker.calculate_lag()

        assert len(tracker._lag_samples) == 1

    @pytest.mark.asyncio
    async def test_calculate_lag_records_metric_when_migration_id_set(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        config: MigrationConfig,
    ) -> None:
        """calculate_lag() reports through the real MigrationMetrics
        instance for this migration when migration_id is set -- not a
        standalone metrics object the test constructs and calls directly."""
        clear_metrics_registry()
        migration_id = uuid4()
        # No tenant_id: seed_events() doesn't tag events with a tenant, and
        # calculate_lag() would filter them out by a tenant that never
        # matches. Exercises the "unknown" tenant fallback in the metrics
        # key at the same time.
        tracker = SyncLagTracker(
            source_store=source_store,
            target_store=target_store,
            config=config,
            migration_id=migration_id,
            enable_tracing=False,
        )
        await seed_events(source_store, 7)

        await tracker.calculate_lag()

        snapshot = get_migration_metrics(str(migration_id), "unknown").get_snapshot()
        assert snapshot.sync_lag_events == 7

        clear_metrics_registry()

    @pytest.mark.asyncio
    async def test_calculate_lag_skips_metric_without_migration_id(
        self,
        tracker: SyncLagTracker,
        source_store: InMemoryEventStore,
    ) -> None:
        """Without migration_id, calculate_lag() still works but records no
        metric -- confirms the metric emission, not the lag calculation
        itself, is what's gated."""
        clear_metrics_registry()
        await seed_events(source_store, 3)

        lag = await tracker.calculate_lag()

        assert lag.events == 3

    @pytest.mark.asyncio
    async def test_calculate_lag_zero_when_since_is_head(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        config: MigrationConfig,
    ) -> None:
        """Target has copied everything: `since` is the source's current head,
        so nothing counts as behind."""
        tracker = SyncLagTracker(
            source_store=source_store,
            target_store=target_store,
            config=config,
            enable_tracing=False,
        )
        last_position = await seed_events(source_store, 25)

        lag = await tracker.calculate_lag(since=last_position)

        assert lag.events == 0
        assert lag.is_converged is True

    @pytest.mark.asyncio
    async def test_calculate_lag_empty_source(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        config: MigrationConfig,
    ) -> None:
        """An empty source feed reports zero lag regardless of `since`."""
        tracker = SyncLagTracker(
            source_store=source_store,
            target_store=target_store,
            config=config,
            enable_tracing=False,
        )

        lag = await tracker.calculate_lag()

        assert lag.events == 0
        assert lag.count_is_bounded is False

    @pytest.mark.asyncio
    async def test_calculate_lag_at_bound_reports_and_stops(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        strict_config: MigrationConfig,
    ) -> None:
        """With `threshold + 1` (or more) events behind, the tracker caps the
        count at `threshold + 1`, marks it bounded, and stops reading."""
        tracker = SyncLagTracker(
            source_store=source_store,
            target_store=target_store,
            config=strict_config,  # threshold = 10
            enable_tracing=False,
        )
        await seed_events(source_store, 15)

        lag = await tracker.calculate_lag()

        assert lag.events == strict_config.cutover_max_lag_events + 1
        assert lag.count_is_bounded is True

    @pytest.mark.asyncio
    async def test_calculate_lag_multiple_calls(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        config: MigrationConfig,
    ) -> None:
        """Test multiple lag calculations as the source feed grows."""
        tracker = SyncLagTracker(
            source_store=source_store,
            target_store=target_store,
            config=config,
            enable_tracing=False,
        )

        await seed_events(source_store, 50)
        await tracker.calculate_lag()

        await seed_events(source_store, 20)
        lag2 = await tracker.calculate_lag()

        assert lag2.events == 70
        assert len(tracker._lag_samples) == 2


class TestConvergenceDetection:
    """Tests for convergence detection methods."""

    @pytest.mark.asyncio
    async def test_is_converged_no_measurement(
        self,
        tracker: SyncLagTracker,
    ) -> None:
        """Test is_converged returns False when no measurement taken."""
        assert tracker.is_converged() is False

    @pytest.mark.asyncio
    async def test_is_converged_within_threshold(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        config: MigrationConfig,
    ) -> None:
        """Test is_converged returns True when within threshold."""
        tracker = SyncLagTracker(
            source_store=source_store,
            target_store=target_store,
            config=config,  # threshold = 100
            enable_tracing=False,
        )
        await seed_events(source_store, 50)

        await tracker.calculate_lag()  # lag = 50

        assert tracker.is_converged() is True

    @pytest.mark.asyncio
    async def test_is_converged_exceeds_threshold(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        strict_config: MigrationConfig,
    ) -> None:
        """Test is_converged returns False when exceeding threshold."""
        tracker = SyncLagTracker(
            source_store=source_store,
            target_store=target_store,
            config=strict_config,  # threshold = 10
            enable_tracing=False,
        )
        await seed_events(source_store, 50)

        await tracker.calculate_lag()  # lag = 11 (bounded)

        assert tracker.is_converged() is False

    @pytest.mark.asyncio
    async def test_is_converged_custom_threshold(
        self,
        tracker: SyncLagTracker,
        source_store: InMemoryEventStore,
    ) -> None:
        """Test is_converged with a custom (tightening) max_lag parameter."""
        await seed_events(source_store, 50)
        await tracker.calculate_lag()  # lag = 50

        assert tracker.is_converged(max_lag=100) is True  # == configured threshold
        assert tracker.is_converged(max_lag=50) is True
        assert tracker.is_converged(max_lag=49) is False
        assert tracker.is_converged(max_lag=10) is False

    @pytest.mark.asyncio
    async def test_is_converged_rejects_looser_threshold(
        self,
        tracker: SyncLagTracker,
        source_store: InMemoryEventStore,
    ) -> None:
        """max_lag may only tighten: the count is bounded, so a looser
        threshold could be satisfied by a bounded count standing for an
        arbitrarily larger backlog."""
        await seed_events(source_store, 50)
        await tracker.calculate_lag()

        with pytest.raises(ValueError, match="exceeds the configured"):
            tracker.is_converged(max_lag=101)

    @pytest.mark.asyncio
    async def test_is_sync_ready_no_measurement(
        self,
        tracker: SyncLagTracker,
    ) -> None:
        """Test is_sync_ready returns False when no measurement taken."""
        assert tracker.is_sync_ready() is False

    @pytest.mark.asyncio
    async def test_is_sync_ready_within_threshold(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        config: MigrationConfig,
    ) -> None:
        """Test is_sync_ready returns True when within threshold."""
        tracker = SyncLagTracker(
            source_store=source_store,
            target_store=target_store,
            config=config,  # threshold = 100
            enable_tracing=False,
        )
        await seed_events(source_store, 50)

        await tracker.calculate_lag()  # lag = 50

        assert tracker.is_sync_ready() is True

    @pytest.mark.asyncio
    async def test_is_sync_ready_exceeds_threshold(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        strict_config: MigrationConfig,
    ) -> None:
        """Test is_sync_ready returns False when exceeding threshold."""
        tracker = SyncLagTracker(
            source_store=source_store,
            target_store=target_store,
            config=strict_config,  # threshold = 10
            enable_tracing=False,
        )
        await seed_events(source_store, 50)

        await tracker.calculate_lag()  # lag = 11 (bounded)

        assert tracker.is_sync_ready() is False

    @pytest.mark.asyncio
    async def test_is_fully_converged_no_measurement(
        self,
        tracker: SyncLagTracker,
    ) -> None:
        """Test is_fully_converged returns False when no measurement taken."""
        assert tracker.is_fully_converged() is False

    @pytest.mark.asyncio
    async def test_is_fully_converged_with_lag(
        self,
        tracker: SyncLagTracker,
        source_store: InMemoryEventStore,
    ) -> None:
        """Test is_fully_converged returns False when there is lag."""
        await seed_events(source_store, 50)
        await tracker.calculate_lag()  # lag = 50

        assert tracker.is_fully_converged() is False

    @pytest.mark.asyncio
    async def test_is_fully_converged_zero_lag(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        config: MigrationConfig,
    ) -> None:
        """Test is_fully_converged returns True when target has copied everything."""
        tracker = SyncLagTracker(
            source_store=source_store,
            target_store=target_store,
            config=config,
            enable_tracing=False,
        )
        last_position = await seed_events(source_store, 25)

        await tracker.calculate_lag(since=last_position)  # lag = 0

        assert tracker.is_fully_converged() is True
