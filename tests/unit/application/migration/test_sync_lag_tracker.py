"""
Unit tests for SyncLagTracker.

Tests cover:
- SyncLagTracker initialization
- Lag calculation as a count of source-feed events after `since`
- Convergence detection (is_converged, is_sync_ready)
- Lag statistics (average, max, min)
- Convergence trend detection
- Manual lag recording
- Sample history management
- Tracing integration
"""

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest

from eventsource.adapters.memory.store import InMemoryEventStore
from eventsource.application.migration.sync_lag_tracker import (
    LagSample,
    LagStats,
    SyncLagTracker,
)
from eventsource.domain import StreamId
from eventsource.domain.event import DomainEvent
from eventsource.ports import ExpectedVersion, Position
from eventsource.ports.migration.models import MigrationConfig, SyncLag

# =============================================================================
# Test Events
# =============================================================================


class LagTestEvent(DomainEvent):
    """Test event for unit tests."""

    event_type: str = "LagTestEvent"
    aggregate_type: str = "LagTestAggregate"
    data: str = "test"


def sid(category: str = "LagTestAggregate") -> StreamId:
    """Build a fresh StreamId for tests."""
    return StreamId(aggregate_id=uuid4(), category=category)


async def seed_events(store: InMemoryEventStore, count: int) -> Position | None:
    """Append `count` events (one per stream) to `store`.

    Returns the position of the last appended event, or None if count == 0.
    """
    last_position: Position | None = None
    for _ in range(count):
        stream = sid()
        result = await store.append(
            stream, [LagTestEvent(aggregate_id=stream.aggregate_id)], ExpectedVersion.any_()
        )
        last_position = result.position
    return last_position


# =============================================================================
# Test Fixtures
# =============================================================================


@pytest.fixture
def source_store() -> InMemoryEventStore:
    """Create an empty source event store."""
    return InMemoryEventStore(store_id="source")


@pytest.fixture
def target_store() -> InMemoryEventStore:
    """Create an empty target event store."""
    return InMemoryEventStore(store_id="target")


@pytest.fixture
def config() -> MigrationConfig:
    """Create a migration config with default thresholds."""
    return MigrationConfig(cutover_max_lag_events=100)


@pytest.fixture
def strict_config() -> MigrationConfig:
    """Create a migration config with strict threshold."""
    return MigrationConfig(cutover_max_lag_events=10)


@pytest.fixture
def tracker(
    source_store: InMemoryEventStore,
    target_store: InMemoryEventStore,
    config: MigrationConfig,
) -> SyncLagTracker:
    """Create a SyncLagTracker with real in-memory stores, source 50 events ahead."""
    return SyncLagTracker(
        source_store=source_store,
        target_store=target_store,
        config=config,
        enable_tracing=False,
    )


# =============================================================================
# Test LagSample Dataclass
# =============================================================================


class TestLagSample:
    """Tests for LagSample dataclass."""

    def test_lag_sample_creation(self) -> None:
        """Test creating a LagSample."""
        lag = SyncLag(
            events=50,
            source_position=Position(store_id="source", key=(100,)),
            target_position=Position(store_id="target", key=(50,)),
            timestamp=datetime.now(UTC),
        )
        sample = LagSample(lag=lag)

        assert sample.lag == lag
        assert sample.sampled_at is not None

    def test_lag_sample_with_custom_timestamp(self) -> None:
        """Test creating a LagSample with custom timestamp."""
        lag = SyncLag(
            events=50,
            source_position=Position(store_id="source", key=(100,)),
            target_position=Position(store_id="target", key=(50,)),
            timestamp=datetime.now(UTC),
        )
        custom_time = datetime(2024, 1, 1, 12, 0, 0, tzinfo=UTC)
        sample = LagSample(lag=lag, sampled_at=custom_time)

        assert sample.sampled_at == custom_time


# =============================================================================
# Test LagStats Dataclass
# =============================================================================


class TestLagStats:
    """Tests for LagStats dataclass."""

    def test_lag_stats_creation(self) -> None:
        """Test creating LagStats."""
        now = datetime.now(UTC)
        stats = LagStats(
            current_lag=50,
            average_lag=45.5,
            max_lag=100,
            min_lag=10,
            sample_count=20,
            first_sample_at=now - timedelta(hours=1),
            last_sample_at=now,
            is_converging=True,
        )

        assert stats.current_lag == 50
        assert stats.average_lag == 45.5
        assert stats.max_lag == 100
        assert stats.min_lag == 10
        assert stats.sample_count == 20
        assert stats.is_converging is True

    def test_lag_stats_to_dict(self) -> None:
        """Test converting LagStats to dictionary."""
        now = datetime.now(UTC)
        stats = LagStats(
            current_lag=50,
            average_lag=45.5,
            max_lag=100,
            min_lag=10,
            sample_count=20,
            first_sample_at=now,
            last_sample_at=now,
            is_converging=False,
        )

        result = stats.to_dict()

        assert result["current_lag"] == 50
        assert result["average_lag"] == 45.5
        assert result["max_lag"] == 100
        assert result["min_lag"] == 10
        assert result["sample_count"] == 20
        assert result["is_converging"] is False
        assert result["first_sample_at"] == now.isoformat()
        assert result["last_sample_at"] == now.isoformat()

    def test_lag_stats_to_dict_with_none_timestamps(self) -> None:
        """Test converting LagStats with None timestamps."""
        stats = LagStats(
            current_lag=0,
            average_lag=0.0,
            max_lag=0,
            min_lag=0,
            sample_count=0,
            first_sample_at=None,
            last_sample_at=None,
        )

        result = stats.to_dict()

        assert result["first_sample_at"] is None
        assert result["last_sample_at"] is None


# =============================================================================
# Test SyncLagTracker Initialization
# =============================================================================


class TestSyncLagTrackerInit:
    """Tests for SyncLagTracker initialization."""

    def test_init_with_defaults(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
    ) -> None:
        """Test initialization with default parameters."""
        tracker = SyncLagTracker(
            source_store=source_store,
            target_store=target_store,
        )

        assert tracker.source_store is source_store
        assert tracker.target_store is target_store
        assert tracker.config is not None
        assert tracker.tenant_id is None
        assert tracker.current_lag is None
        assert tracker._max_sample_history == 100

    def test_init_with_config(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        strict_config: MigrationConfig,
    ) -> None:
        """Test initialization with custom config."""
        tracker = SyncLagTracker(
            source_store=source_store,
            target_store=target_store,
            config=strict_config,
        )

        assert tracker.config is strict_config
        assert tracker.sync_threshold == 10

    def test_init_with_tenant_id(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
    ) -> None:
        """Test initialization with tenant ID."""
        tenant_id = uuid4()
        tracker = SyncLagTracker(
            source_store=source_store,
            target_store=target_store,
            tenant_id=tenant_id,
        )

        assert tracker.tenant_id == tenant_id

    def test_init_with_custom_sample_history(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
    ) -> None:
        """Test initialization with custom max sample history."""
        tracker = SyncLagTracker(
            source_store=source_store,
            target_store=target_store,
            max_sample_history=50,
        )

        assert tracker._max_sample_history == 50

    def test_init_with_tracing_disabled(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
    ) -> None:
        """Test initialization with tracing disabled."""
        tracker = SyncLagTracker(
            source_store=source_store,
            target_store=target_store,
            enable_tracing=False,
        )

        assert tracker._enable_tracing is False

    def test_properties(
        self,
        tracker: SyncLagTracker,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        config: MigrationConfig,
    ) -> None:
        """Test property accessors."""
        assert tracker.source_store is source_store
        assert tracker.target_store is target_store
        assert tracker.config is config
        assert tracker.sync_threshold == 100


# =============================================================================
# Test Lag Calculation
# =============================================================================


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


# =============================================================================
# Test Convergence Detection
# =============================================================================


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


# =============================================================================
# Test Lag Statistics
# =============================================================================


class TestTrackerLagStats:
    """Tests for lag statistics methods."""

    def test_get_lag_stats_no_samples(
        self,
        tracker: SyncLagTracker,
    ) -> None:
        """Test get_lag_stats with no samples."""
        stats = tracker.get_lag_stats()

        assert stats.current_lag == 0
        assert stats.average_lag == 0.0
        assert stats.max_lag == 0
        assert stats.min_lag == 0
        assert stats.sample_count == 0
        assert stats.first_sample_at is None
        assert stats.last_sample_at is None
        assert stats.is_converging is False

    @pytest.mark.asyncio
    async def test_get_lag_stats_single_sample(
        self,
        tracker: SyncLagTracker,
        source_store: InMemoryEventStore,
    ) -> None:
        """Test get_lag_stats with single sample."""
        await seed_events(source_store, 50)
        await tracker.calculate_lag()  # lag = 50

        stats = tracker.get_lag_stats()

        assert stats.current_lag == 50
        assert stats.average_lag == 50.0
        assert stats.max_lag == 50
        assert stats.min_lag == 50
        assert stats.sample_count == 1
        assert stats.first_sample_at is not None
        assert stats.last_sample_at is not None

    @pytest.mark.asyncio
    async def test_get_lag_stats_multiple_samples(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        config: MigrationConfig,
    ) -> None:
        """Test get_lag_stats with multiple samples as target catches up."""
        tracker = SyncLagTracker(
            source_store=source_store,
            target_store=target_store,
            config=config,
            enable_tracing=False,
        )

        positions = []
        for _ in range(90):
            result = await source_store.append(
                (stream := sid()),
                [LagTestEvent(aggregate_id=stream.aggregate_id)],
                ExpectedVersion.any_(),
            )
            positions.append(result.position)

        # First measurement: nothing copied yet -> 90 behind (bounded at 101)
        await tracker.calculate_lag()

        # Second measurement: target has copied the first 60 -> 30 behind
        await tracker.calculate_lag(since=positions[59])

        # Third measurement: target has copied the first 80 -> 10 behind
        await tracker.calculate_lag(since=positions[79])

        stats = tracker.get_lag_stats()

        assert stats.current_lag == 10
        assert stats.sample_count == 3
        assert stats.max_lag == 90
        assert stats.min_lag == 10

    @pytest.mark.asyncio
    async def test_convergence_trend_detection(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        config: MigrationConfig,
    ) -> None:
        """Test that convergence trend is detected correctly."""
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

        # Decreasing lag pattern (converging): target catches up over time
        copied_counts = [50, 60, 70, 80, 90, 95]
        for copied in copied_counts:
            since = positions[copied - 1]
            await tracker.calculate_lag(since=since)

        stats = tracker.get_lag_stats()

        assert stats.is_converging is True

    @pytest.mark.asyncio
    async def test_non_convergence_trend_detection(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        config: MigrationConfig,
    ) -> None:
        """Test that non-convergence (diverging) is detected."""
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

        # Increasing lag pattern (diverging): target falls further behind
        copied_counts = [90, 80, 70, 60, 50, 40]
        for copied in copied_counts:
            since = positions[copied - 1]
            await tracker.calculate_lag(since=since)

        stats = tracker.get_lag_stats()

        assert stats.is_converging is False


# =============================================================================
# Test Sample History Management
# =============================================================================


class TestSampleHistoryManagement:
    """Tests for sample history management."""

    @pytest.mark.asyncio
    async def test_get_sample_history(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        config: MigrationConfig,
    ) -> None:
        """Test getting sample history."""
        tracker = SyncLagTracker(
            source_store=source_store,
            target_store=target_store,
            config=config,
            enable_tracing=False,
        )

        positions = []
        for _ in range(50):
            result = await source_store.append(
                (stream := sid()),
                [LagTestEvent(aggregate_id=stream.aggregate_id)],
                ExpectedVersion.any_(),
            )
            positions.append(result.position)

        await tracker.calculate_lag()
        await tracker.calculate_lag(since=positions[19])

        history = tracker.get_sample_history()

        assert len(history) == 2
        assert all(isinstance(lag, SyncLag) for lag in history)
        assert history[0].events == 50
        assert history[1].events == 30

    def test_clear_history(
        self,
        tracker: SyncLagTracker,
    ) -> None:
        """Test clearing sample history."""
        # Add some samples manually
        lag = SyncLag(
            events=50,
            source_position=Position(store_id="source", key=(100,)),
            target_position=Position(store_id="target", key=(50,)),
            timestamp=datetime.now(UTC),
        )
        tracker.record_lag(lag)

        assert len(tracker._lag_samples) == 1
        assert tracker.current_lag is not None

        cleared = tracker.clear_history()

        assert cleared == 1
        assert len(tracker._lag_samples) == 0
        assert tracker.current_lag is None

    @pytest.mark.asyncio
    async def test_sample_history_max_size(
        self,
        source_store: InMemoryEventStore,
        target_store: InMemoryEventStore,
        config: MigrationConfig,
    ) -> None:
        """Test that sample history respects max size."""
        max_history = 5
        tracker = SyncLagTracker(
            source_store=source_store,
            target_store=target_store,
            config=config,
            enable_tracing=False,
            max_sample_history=max_history,
        )

        # Take more measurements than max, growing the source feed each time.
        for _ in range(max_history + 3):
            await seed_events(source_store, 1)
            await tracker.calculate_lag()

        assert len(tracker._lag_samples) == max_history


# =============================================================================
# Test Manual Lag Recording
# =============================================================================


class TestManualLagRecording:
    """Tests for manual lag recording."""

    def test_record_lag(
        self,
        tracker: SyncLagTracker,
    ) -> None:
        """Test manually recording a lag measurement."""
        lag = SyncLag(
            events=25,
            source_position=Position(store_id="source", key=(100,)),
            target_position=Position(store_id="target", key=(75,)),
            timestamp=datetime.now(UTC),
        )

        tracker.record_lag(lag)

        assert tracker.current_lag is lag
        assert len(tracker._lag_samples) == 1
        assert tracker._lag_samples[0].lag is lag

    def test_record_lag_multiple(
        self,
        tracker: SyncLagTracker,
    ) -> None:
        """Test recording multiple lag measurements."""
        for i in range(3):
            lag = SyncLag(
                events=50 - (i * 10),
                source_position=Position(store_id="source", key=(100,)),
                target_position=Position(store_id="target", key=(50 + (i * 10),)),
                timestamp=datetime.now(UTC),
            )
            tracker.record_lag(lag)

        assert len(tracker._lag_samples) == 3
        assert tracker.current_lag.events == 30  # Last recorded

    def test_record_lag_updates_stats(
        self,
        tracker: SyncLagTracker,
    ) -> None:
        """Test that recorded lags update statistics."""
        lags = [50, 40, 30, 20, 10]
        for lag_events in lags:
            lag = SyncLag(
                events=lag_events,
                source_position=Position(store_id="source", key=(100,)),
                target_position=Position(store_id="target", key=(100 - lag_events,)),
                timestamp=datetime.now(UTC),
            )
            tracker.record_lag(lag)

        stats = tracker.get_lag_stats()

        assert stats.current_lag == 10
        assert stats.average_lag == 30.0  # (50+40+30+20+10) / 5
        assert stats.max_lag == 50
        assert stats.min_lag == 10


# =============================================================================
# Test Edge Cases
# =============================================================================


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


# =============================================================================
# Test with Tenant ID
# =============================================================================


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


# =============================================================================
# Test Integration Scenarios
# =============================================================================


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
